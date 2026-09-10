/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Durable state for one checkpoint-gated scaling operation. */
@Data
@NoArgsConstructor
@JsonIgnoreProperties("terminal")
public class CheckpointRescaleTransaction {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    /** Transaction phases are monotonic except for an explicit retry. */
    public enum Phase {
        WAITING_PRODUCER_PAUSE,
        WAITING_CHECKPOINT,
        CHECKPOINT_TRIGGERED,
        READY_TO_APPLY,
        APPLYING,
        RESTORING,
        VERIFYING,
        WAITING_PRODUCER_RESUME,
        COMPLETED,
        FAILED
    }

    private int schemaVersion = CURRENT_SCHEMA_VERSION;
    private Phase phase;
    private String transactionId;
    private String jobId;
    private long initialRunningTimestamp;
    private Map<String, String> previousParallelismOverrides = new HashMap<>();
    private Map<String, String> previousResourceProfileOverrides = new HashMap<>();
    private Map<String, String> targetParallelismOverrides = new HashMap<>();
    private Map<String, String> targetResourceProfileOverrides = new HashMap<>();
    private Instant decisionTimestamp;
    private Instant phaseTimestamp;
    private String checkpointTriggerId;
    private Long completedCheckpointId;
    private Instant checkpointCompletedTimestamp;
    private Instant appliedTimestamp;
    private Long restoredRunningTimestamp;
    private Long observedRestartDurationMillis;
    private boolean targetApplied;
    private long handledRetryNonce;
    private long handledAbortNonce;
    private String error;

    public boolean wasApplied() {
        return targetApplied;
    }

    /** Rejects incomplete or incompatible persisted state instead of resuming it partially. */
    public void validate() {
        checkState(
                schemaVersion == CURRENT_SCHEMA_VERSION,
                "Unsupported checkpoint rescale schema version: %s",
                schemaVersion);
        checkState(phase != null, "Checkpoint rescale phase is missing");
        checkState(jobId != null, "Checkpoint rescale job ID is missing");
        checkState(decisionTimestamp != null, "Checkpoint rescale decision timestamp is missing");
        checkState(phaseTimestamp != null, "Checkpoint rescale phase timestamp is missing");
        checkState(previousParallelismOverrides != null, "Previous parallelism map is missing");
        checkState(previousResourceProfileOverrides != null, "Previous profile map is missing");
        checkState(targetParallelismOverrides != null, "Target parallelism map is missing");
        checkState(targetResourceProfileOverrides != null, "Target profile map is missing");
        if (phase == Phase.COMPLETED) {
            checkState(appliedTimestamp != null, "Completed rescale is missing apply timestamp");
            checkState(
                    restoredRunningTimestamp != null,
                    "Completed rescale is missing restored RUNNING timestamp");
            checkState(
                    observedRestartDurationMillis != null,
                    "Completed rescale is missing observed restart duration");
            checkState(
                    observedRestartDurationMillis >= 0,
                    "Observed restart duration must not be negative");
        }
    }
}
