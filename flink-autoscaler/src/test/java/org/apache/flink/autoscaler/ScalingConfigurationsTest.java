/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ScalingConfigurations}. */
class ScalingConfigurationsTest {

    @Test
    void testUnchangedVertexMemoryLevelIsCarriedForward() {
        var configurations = new ScalingConfigurations();
        var jobId = new JobID();
        var scalingVertex = new JobVertexID();
        var unchangedVertex = new JobVertexID();
        var vertexMetrics =
                Map.of(
                        scalingVertex, metrics(1, 10_000.0),
                        unchangedVertex, metrics(2, 20_000.0));

        var previous =
                configurations.setCurrentConfiguration(jobId, vertexMetrics, Map.of(), 0);
        previous.getFromJobVertexId(scalingVertex).setMemoryLevel(1);
        previous.getFromJobVertexId(unchangedVertex).setMemoryLevel(2);

        var current =
                configurations.setCurrentConfiguration(
                        jobId,
                        vertexMetrics,
                        Map.of(
                                scalingVertex,
                                new ScalingSummary(1, 4, vertexMetrics.get(scalingVertex))),
                        1);

        assertThat(current.getFromJobVertexId(scalingVertex).getParallelism()).isEqualTo(4);
        assertThat(current.getFromJobVertexId(scalingVertex).getMemoryLevel()).isEqualTo(1);
        assertThat(current.getFromJobVertexId(unchangedVertex).getParallelism()).isEqualTo(2);
        assertThat(current.getFromJobVertexId(unchangedVertex).getMemoryLevel()).isEqualTo(2);
    }

    @Test
    void testValueStateLatencyIsIncluded() {
        var vertex = new JobVertexID();
        var metrics = metrics(1, 10_000.0);
        metrics.put(
                ScalingMetric.VALUE_STATE_GET_MEAN_LATENCY,
                EvaluatedScalingMetric.avg(42_000.0));

        var configuration =
                new ScalingConfigurations.ScalingConfiguration(
                        Map.of(vertex, metrics), Map.of());

        assertThat(configuration.getFromJobVertexId(vertex).getAvgStateLatency())
                .isEqualTo(42_000.0);
    }

    @Test
    void testValueStateLatencyDoesNotChangeExistingStateTypePriority() {
        var vertex = new JobVertexID();
        var metrics = metrics(1, 10_000.0);
        metrics.put(
                ScalingMetric.MAP_STATE_GET_MEAN_LATENCY,
                EvaluatedScalingMetric.avg(24_000.0));
        metrics.put(
                ScalingMetric.VALUE_STATE_GET_MEAN_LATENCY,
                EvaluatedScalingMetric.avg(42_000.0));

        var configuration =
                new ScalingConfigurations.ScalingConfiguration(
                        Map.of(vertex, metrics), Map.of());

        assertThat(configuration.getFromJobVertexId(vertex).getAvgStateLatency())
                .isEqualTo(24_000.0);
    }

    private static Map<ScalingMetric, EvaluatedScalingMetric> metrics(
            int parallelism, double processingRate) {
        var metrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        metrics.put(ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(parallelism));
        metrics.put(
                ScalingMetric.TRUE_PROCESSING_RATE,
                EvaluatedScalingMetric.avg(processingRate));
        return metrics;
    }
}
