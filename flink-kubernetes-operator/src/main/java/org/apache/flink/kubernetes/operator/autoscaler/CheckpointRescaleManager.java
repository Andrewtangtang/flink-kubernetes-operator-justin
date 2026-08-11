/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.CheckpointRescaleTransaction;
import org.apache.flink.autoscaler.CheckpointRescaleTransaction.Phase;
import org.apache.flink.autoscaler.ScalingDecisionGate;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_CHECKPOINT_TIMEOUT;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.CHECKPOINT_RESCALE_RESTORE_TIMEOUT;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.FLINK_CLIENT_TIMEOUT;

/** Coordinates a durable checkpoint before an autoscaler in-place rescale. */
public class CheckpointRescaleManager
        implements ScalingDecisionGate<KubernetesJobAutoScalerContext> {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointRescaleManager.class);

    public static final String PHASE_ANNOTATION =
            "autoscaling.flink.apache.org/checkpoint-rescale-phase";
    public static final String RETRY_NONCE_ANNOTATION =
            "autoscaling.flink.apache.org/checkpoint-rescale-retry-nonce";
    public static final String ABORT_NONCE_ANNOTATION =
            "autoscaling.flink.apache.org/checkpoint-rescale-abort-nonce";

    private final KubernetesAutoScalerStateStore stateStore;
    private Clock clock = Clock.systemUTC();

    public CheckpointRescaleManager(KubernetesAutoScalerStateStore stateStore) {
        this.stateStore = stateStore;
    }

    @Override
    public boolean blocksNewDecision(KubernetesJobAutoScalerContext context) throws Exception {
        if (!enabled(context)) {
            return false;
        }
        var transaction = stateStore.getCheckpointRescaleTransaction(context);
        if (transaction.isEmpty()) {
            return false;
        }
        if (handleAction(context, transaction.get())) {
            // Do not make a fresh scaling decision in the same reconcile cycle as a manual
            // transaction action. A retry can still advance its frozen target in the realizer.
            return true;
        }
        return transaction.isPresent() && transaction.get().getPhase() != Phase.COMPLETED;
    }

    @Override
    public void handleScalingFailure(
            KubernetesJobAutoScalerContext context, Throwable failure) throws Exception {
        if (!enabled(context)) {
            return;
        }

        var transaction = stateStore.getCheckpointRescaleTransaction(context);
        if (transaction.isEmpty() || transaction.get().getPhase() != Phase.APPLYING) {
            LOG.debug("Ignoring a scaling failure without an applying checkpoint transaction");
            return;
        }

        fail(context, transaction.get(), "Failed to apply checkpoint rescale target", failure);
    }

    @Override
    public boolean blocksScalingApplication(KubernetesJobAutoScalerContext context)
            throws Exception {
        if (!enabled(context)) {
            return false;
        }
        return stateStore
                .getCheckpointRescaleTransaction(context)
                .map(transaction -> transaction.getPhase() == Phase.FAILED)
                .orElse(false);
    }

    /**
     * Advances the transaction and returns true only when the frozen target may be written to the
     * FlinkDeployment spec.
     */
    public boolean prepareScaling(
            KubernetesJobAutoScalerContext context,
            Map<String, String> targetParallelism,
            Map<String, String> targetResourceProfiles)
            throws Exception {
        if (!enabled(context)) {
            return true;
        }

        var transactionOpt = stateStore.getCheckpointRescaleTransaction(context);
        if (transactionOpt.isEmpty()
                || (transactionOpt.get().getPhase() == Phase.COMPLETED
                        && !sameTarget(
                                transactionOpt.get(),
                                targetParallelism,
                                targetResourceProfiles))) {
            transactionOpt =
                    startTransaction(context, targetParallelism, targetResourceProfiles);
        }
        if (transactionOpt.isEmpty()) {
            return false;
        }

        var transaction = transactionOpt.get();
        if (handleAction(context, transaction)) {
            return false;
        }
        if (transaction.getPhase() == Phase.FAILED) {
            LOG.error("Checkpoint rescale is blocked: {}", transaction.getError());
            return false;
        }
        if (transaction.getPhase() == Phase.COMPLETED) {
            return true;
        }
        if (!sameTarget(transaction, targetParallelism, targetResourceProfiles)) {
            LOG.warn("Ignoring a changed target while checkpoint rescale transaction is active");
        }

        try {
            switch (transaction.getPhase()) {
                case WAITING_CHECKPOINT:
                    waitForCheckpointSlot(context, transaction);
                    return false;
                case CHECKPOINT_TRIGGERED:
                    pollCheckpoint(context, transaction);
                    return false;
                case READY_TO_APPLY:
                    transaction.setAppliedTimestamp(clock.instant());
                    transaction.setTargetApplied(true);
                    transition(context, transaction, Phase.APPLYING);
                    return true;
                case APPLYING:
                    if (!currentJobMatches(context, transaction)) {
                        fail(context, transaction, "Job ID changed during in-place rescale", null);
                        return false;
                    }
                    if (!observesTarget(context, transaction)) {
                        return true;
                    }
                    verifyRestoredTarget(context, transaction);
                    return false;
                case RESTORING:
                case VERIFYING:
                    verifyRestoredTarget(context, transaction);
                    return false;
                default:
                    return false;
            }
        } catch (Exception e) {
            fail(
                    context,
                    transaction,
                    "Checkpoint rescale phase " + transaction.getPhase() + " failed",
                    e);
            return false;
        }
    }

    private Optional<CheckpointRescaleTransaction> startTransaction(
            KubernetesJobAutoScalerContext context,
            Map<String, String> targetParallelism,
            Map<String, String> targetResourceProfiles)
            throws Exception {
        if (context.getJobID() == null || context.getJobStatus() != JobStatus.RUNNING) {
            LOG.info("Waiting for a running job before starting checkpoint rescale");
            return Optional.empty();
        }

        var observeConfig = context.getResourceContext().getObserveConfig();
        var previousParallelism =
                observeConfig == null
                        ? Map.<String, String>of()
                        : observeConfig
                                .getOptional(PipelineOptions.PARALLELISM_OVERRIDES)
                                .orElse(Map.of());
        var previousProfiles =
                observeConfig == null
                        ? Map.<String, String>of()
                        : observeConfig
                                .getOptional(
                                        KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES)
                                .orElse(Map.of());

        if (previousParallelism.equals(targetParallelism)
                && previousProfiles.equals(targetResourceProfiles)) {
            return Optional.empty();
        }

        var transaction = new CheckpointRescaleTransaction();
        transaction.setPhase(Phase.WAITING_CHECKPOINT);
        transaction.setJobId(context.getJobID().toHexString());
        transaction.setPreviousParallelismOverrides(new HashMap<>(previousParallelism));
        transaction.setPreviousResourceProfileOverrides(new HashMap<>(previousProfiles));
        transaction.setTargetParallelismOverrides(new HashMap<>(targetParallelism));
        transaction.setTargetResourceProfileOverrides(new HashMap<>(targetResourceProfiles));
        transaction.setDecisionTimestamp(clock.instant());
        transaction.setPhaseTimestamp(clock.instant());
        persist(context, transaction);
        try {
            transaction.setInitialRunningTimestamp(getRunningTimestamp(context));
            persist(context, transaction);
        } catch (Exception e) {
            fail(context, transaction, "Cannot read initial RUNNING timestamp", e);
        }
        if (transaction.getPhase() != Phase.FAILED) {
            LOG.info("Started checkpoint rescale transaction for job {}", transaction.getJobId());
        }
        return Optional.of(transaction);
    }

    private void waitForCheckpointSlot(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_CHECKPOINT_TIMEOUT)) {
            return;
        }
        var checkpointInfo =
                context.getResourceContext()
                        .getFlinkService()
                        .getCheckpointInfo(context.getJobID(), runtimeConfig(context));
        if (checkpointInfo.f1.isPresent()) {
            LOG.info(
                    "Waiting for checkpoint {} before triggering the rescale checkpoint",
                    checkpointInfo.f1.get().getId());
            return;
        }

        try {
            transaction.setCheckpointTriggerId(
                    context.getResourceContext()
                            .getFlinkService()
                            .triggerCheckpoint(
                                    transaction.getJobId(),
                                    CheckpointType.CONFIGURED,
                                    runtimeConfig(context)));
            transition(context, transaction, Phase.CHECKPOINT_TRIGGERED);
        } catch (Exception e) {
            fail(context, transaction, "Checkpoint trigger failed", e);
        }
    }

    private void pollCheckpoint(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_CHECKPOINT_TIMEOUT)) {
            return;
        }
        CheckpointFetchResult result =
                context.getResourceContext()
                        .getFlinkService()
                        .fetchCheckpointInfo(
                                transaction.getCheckpointTriggerId(),
                                transaction.getJobId(),
                                runtimeConfig(context));
        if (result.isPending()) {
            return;
        }
        if (result.getError() != null) {
            fail(context, transaction, "Checkpoint failed: " + result.getError(), null);
            return;
        }
        transaction.setCompletedCheckpointId(result.getCheckpointId());
        transaction.setCheckpointCompletedTimestamp(clock.instant());
        transition(context, transaction, Phase.READY_TO_APPLY);
    }

    private void verifyRestoredTarget(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        if (checkTimeout(context, transaction, CHECKPOINT_RESCALE_RESTORE_TIMEOUT)) {
            return;
        }
        if (!currentJobMatches(context, transaction)) {
            fail(context, transaction, "Job ID changed during in-place rescale", null);
            return;
        }
        if (context.getJobStatus() != JobStatus.RUNNING) {
            transition(context, transaction, Phase.RESTORING);
            return;
        }

        long runningTimestamp = getRunningTimestamp(context);
        if (runningTimestamp <= transaction.getInitialRunningTimestamp()) {
            return;
        }
        transition(context, transaction, Phase.VERIFYING);

        if (!observesTarget(context, transaction)) {
            return;
        }

        recordObservedRestartDuration(context, transaction, runningTimestamp);
        transition(context, transaction, Phase.COMPLETED);
        LOG.info(
                "Checkpoint rescale completed from checkpoint {} in {} milliseconds",
                transaction.getCompletedCheckpointId(),
                transaction.getObservedRestartDurationMillis());
    }

    private void recordObservedRestartDuration(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction,
            long runningTimestamp)
            throws Exception {
        var appliedTimestamp = transaction.getAppliedTimestamp();
        if (appliedTimestamp == null) {
            throw new IllegalStateException("Applied timestamp is unavailable");
        }

        var restoredAt = Instant.ofEpochMilli(runningTimestamp);
        var restartDuration = Duration.between(appliedTimestamp, restoredAt);
        if (restartDuration.isNegative()) {
            throw new IllegalStateException(
                    String.format(
                            "Restored RUNNING timestamp %s precedes apply timestamp %s",
                            restoredAt, appliedTimestamp));
        }

        transaction.setRestoredRunningTimestamp(runningTimestamp);
        transaction.setObservedRestartDurationMillis(restartDuration.toMillis());

        var scalingTracking = stateStore.getScalingTracking(context);
        scalingTracking.recordLatestRestartDuration(restartDuration);
        stateStore.storeScalingTracking(context, scalingTracking);
    }

    private boolean handleAction(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        long retryNonce = annotationNonce(context, RETRY_NONCE_ANNOTATION);
        long abortNonce = annotationNonce(context, ABORT_NONCE_ANNOTATION);
        if (abortNonce > transaction.getHandledAbortNonce()) {
            transaction.setHandledAbortNonce(abortNonce);
            if (transaction.wasApplied()) {
                persist(context, transaction);
                LOG.warn("Ignoring abort because target overrides were already applied");
            } else {
                stateStore.storeParallelismOverrides(
                        context, transaction.getPreviousParallelismOverrides());
                stateStore.storeResourceProfileOverrides(
                        context, transaction.getPreviousResourceProfileOverrides());
                stateStore.removeCheckpointRescaleTransaction(context);
                setPhaseAnnotation(context, null);
                stateStore.flush(context);
                LOG.warn("Aborted checkpoint rescale before applying target overrides");
            }
            return true;
        }
        if (retryNonce > transaction.getHandledRetryNonce()) {
            transaction.setHandledRetryNonce(retryNonce);
            if (transaction.getPhase() == Phase.FAILED) {
                transaction.setError(null);
                if (transaction.wasApplied()) {
                    transition(context, transaction, Phase.APPLYING);
                } else {
                    transaction.setCheckpointTriggerId(null);
                    transaction.setCompletedCheckpointId(null);
                    transaction.setCheckpointCompletedTimestamp(null);
                    transition(context, transaction, Phase.WAITING_CHECKPOINT);
                }
                LOG.warn("Retrying frozen checkpoint rescale target");
            } else {
                persist(context, transaction);
                LOG.warn("Ignoring retry because checkpoint rescale has not failed");
            }
            return true;
        }
        return false;
    }

    private boolean checkTimeout(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction,
            ConfigOption<Duration> timeoutOption)
            throws Exception {
        var deadline =
                transaction
                        .getPhaseTimestamp()
                        .plus(context.getConfiguration().get(timeoutOption));
        if (clock.instant().isAfter(deadline)) {
            fail(
                    context,
                    transaction,
                    "Timed out in checkpoint rescale phase " + transaction.getPhase(),
                    null);
            return true;
        }
        return false;
    }

    long getRunningTimestamp(KubernetesJobAutoScalerContext context) throws Exception {
        try (var client = context.getRestClusterClient()) {
            JobDetailsInfo details =
                    client.getJobDetails(context.getJobID())
                            .get(
                                    context.getConfiguration().get(FLINK_CLIENT_TIMEOUT).toSeconds(),
                                    TimeUnit.SECONDS);
            Long runningTimestamp = details.getTimestamps().get(JobStatus.RUNNING);
            if (runningTimestamp == null) {
                throw new IllegalStateException("RUNNING timestamp is unavailable");
            }
            return runningTimestamp;
        }
    }

    private Configuration runtimeConfig(KubernetesJobAutoScalerContext context) {
        var observeConfig = context.getResourceContext().getObserveConfig();
        return observeConfig == null ? context.getConfiguration() : observeConfig;
    }

    private void transition(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction,
            Phase phase)
            throws Exception {
        if (transaction.getPhase() == phase) {
            return;
        }
        transaction.setPhase(phase);
        transaction.setPhaseTimestamp(clock.instant());
        persist(context, transaction);
    }

    private void fail(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction,
            String message,
            Throwable cause)
            throws Exception {
        transaction.setError(cause == null ? message : message + ": " + rootCauseMessage(cause));
        if (transaction.getPhase() == Phase.FAILED) {
            persist(context, transaction);
        } else {
            transition(context, transaction, Phase.FAILED);
        }
        LOG.error(transaction.getError(), cause);
    }

    private String rootCauseMessage(Throwable failure) {
        var rootCause = failure;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        var message = rootCause.getMessage();
        return rootCause.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message);
    }

    private void persist(
            KubernetesJobAutoScalerContext context, CheckpointRescaleTransaction transaction)
            throws Exception {
        stateStore.storeCheckpointRescaleTransaction(context, transaction);
        setPhaseAnnotation(context, transaction.getPhase());
        stateStore.flush(context);
    }

    private void setPhaseAnnotation(KubernetesJobAutoScalerContext context, Phase phase) {
        var annotations = context.getResource().getMetadata().getAnnotations();
        if (annotations == null) {
            annotations = new HashMap<>();
            context.getResource().getMetadata().setAnnotations(annotations);
        }
        if (phase == null) {
            annotations.remove(PHASE_ANNOTATION);
        } else {
            annotations.put(PHASE_ANNOTATION, phase.name());
        }
    }

    private long annotationNonce(KubernetesJobAutoScalerContext context, String annotation) {
        var value =
                Optional.ofNullable(context.getResource().getMetadata().getAnnotations())
                        .map(values -> values.get(annotation))
                        .orElse(null);
        if (value == null) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            LOG.warn("Ignoring invalid {} annotation value: {}", annotation, value);
            return 0L;
        }
    }

    private boolean enabled(KubernetesJobAutoScalerContext context) {
        return context.getConfiguration().get(CHECKPOINT_RESCALE_ENABLED);
    }

    private boolean sameTarget(
            CheckpointRescaleTransaction transaction,
            Map<String, String> parallelism,
            Map<String, String> resourceProfiles) {
        return transaction.getTargetParallelismOverrides().equals(parallelism)
                && transaction.getTargetResourceProfileOverrides().equals(resourceProfiles);
    }

    private boolean currentJobMatches(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction) {
        return context.getJobID() != null
                && transaction.getJobId().equals(context.getJobID().toHexString());
    }

    private boolean observesTarget(
            KubernetesJobAutoScalerContext context,
            CheckpointRescaleTransaction transaction) {
        var observeConfig = context.getResourceContext().getObserveConfig();
        return observeConfig != null
                && observeConfig
                        .get(PipelineOptions.PARALLELISM_OVERRIDES)
                        .equals(transaction.getTargetParallelismOverrides())
                && observeConfig
                        .get(KubernetesScalingRealizer.RESOURCE_PROFILE_OVERRIDES)
                        .equals(transaction.getTargetResourceProfileOverrides());
    }

    void setClock(Clock clock) {
        this.clock = clock;
    }
}
