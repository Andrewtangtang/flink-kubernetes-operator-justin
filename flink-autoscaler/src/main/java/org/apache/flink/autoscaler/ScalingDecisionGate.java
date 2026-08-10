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

    static <Context> ScalingDecisionGate<Context> open() {
        return context -> false;
    }
}

