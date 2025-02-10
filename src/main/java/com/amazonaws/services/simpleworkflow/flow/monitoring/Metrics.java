/**
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow.monitoring;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A Metrics instance is a producer of measurements. Each metric instance must be closed to report recorded
 * measurements. Once closed, no new measurements will be recorded.
 * <p>
 * Metrics instances are not thread-safe and are intended for use by a single thread. Use {@link #newMetrics()} to create
 * sub-metrics instances to aggregate measurements from multiple threads.
 */
public interface Metrics extends Closeable {

    /**
     * Creates a new Metrics instance that, when closed, passes measurements back to current instance for aggregation.
     *
     * <b>Note:</b> Measurements from child instance will be lost if the current instance is already closed.
     */
    Metrics newMetrics();

    /**
     * Returns the {@link MetricsRegistry} that was used to create this Metrics instance. Calling {@link MetricsRegistry#newMetrics()}
     * will create a new metrics log entry with same basic metadata like Marketplace, Program, etc.
     */
    default MetricsRegistry getMetricsRegistry() {
        return NullMetricsRegistry.getInstance();
    }

    /**
     * Adds the key-value pair as metadata to metrics log. Will only log the latest value if a key gets multiple values.
     * <pre>
     * {@code
     * metrics.addProperty("RequestId", df4d7e06-e91b-408f-b431-24a0f5404cb7);
     * metrics.addProperty("RequestId", 25fbbd70-f9d6-4412-979b-771cb072cddf);
     * }
     * </pre>
     * Results in below line in metrics log:
     * <pre>
     * {@code
     * RequestId=25fbbd70-f9d6-4412-979b-771cb072cddf
     * }
     * </pre>
     */
    void addProperty(String name, String value);

    /**
     * Similar to {@link #addProperty(String, String)}, but allows a key to have multiple values. All values for a key
     * will be logged to metrics log as comma-separated values.
     * <pre>
     * {@code
     * metrics.addAppendableProperty("RequestIds", df4d7e06-e91b-408f-b431-24a0f5404cb7);
     * metrics.addAppendableProperty("RequestIds", 25fbbd70-f9d6-4412-979b-771cb072cddf);
     * }
     * </pre>
     * Results in below line in metrics log:
     * <pre>
     * {@code
     * RequestIds=df4d7e06-e91b-408f-b431-24a0f5404cb7,25fbbd70-f9d6-4412-979b-771cb072cddf
     * }
     * </pre>
     */
    default void addAppendableProperty(String name, String value) {
    }

    void recordCount(String name, double amount);

    void recordCount(String name, double amount, Map<String, String> dimensions);

    void recordCount(String name, boolean success);

    void recordCount(String name, boolean success, Map<String, String> dimensions);

    void record(String name, long amount, TimeUnit unit);

    void record(String name, long amount, TimeUnit unit, Map<String, String> dimensions);

    <T> T recordSupplier(final Supplier<T> supplier, final String operation, final TimeUnit unit);

    void recordRunnable(final Runnable runnable, final String operation, final TimeUnit unit);

    <T> T recordCallable(final Callable<T> callable, final String operation, final TimeUnit unit) throws Exception;

    @Override
    void close();
}
