/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.autoscaler;

/** Prevents a new scaling decision while an earlier decision is being realized. */
@FunctionalInterface
public interface ScalingDecisionGate<Context> {

    boolean blocksNewDecision(Context context) throws Exception;

    /** Records a failure that occurred while applying an allowed scaling decision. */
    default void handleScalingFailure(Context context, Throwable failure) throws Exception {}

    /** Returns whether reconciliation must stop before applying deployment-spec changes. */
    default boolean blocksScalingApplication(Context context) throws Exception {
        return false;
    }

    static <Context> ScalingDecisionGate<Context> open() {
        return context -> false;
    }
}
