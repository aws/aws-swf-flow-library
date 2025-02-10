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

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

public class NullMetricsRegistry implements MetricsRegistry {

    private static final NullMetricsRegistry INSTANCE = new NullMetricsRegistry();

    public static NullMetricsRegistry getInstance() {
        return INSTANCE;
    }

    @Override
    public Metrics newMetrics(final String operation) {
        return NullMetrics.getInstance();
    }

    @Override
    public Metrics newMetrics() {
        return newMetrics(null);
    }

    @Override
    public ExecutorServiceMonitor getExecutorServiceMonitor() {
        return new ExecutorServiceMonitor() {
            @Override
            public ExecutorService monitor(ExecutorService executorService, String name) {
                return executorService;
            }

            @Override
            public void registerCallback(BiConsumer<ExecutorService, String> eventConsumer) {

            }
        };
    }
}
