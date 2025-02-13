/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow.monitoring;

import lombok.Value;

public class ThreadLocalMetrics {

    private static final Metrics NULL_METRICS = NullMetricsRegistry.getInstance().newMetrics();

    private static final ThreadLocal<Context> THREAD_CONTEXT = new ThreadLocal<>();

    private static Context getOrCreateCurrentContext() {
        Context context = THREAD_CONTEXT.get();
        if (context == null) {
            context = new Context(NULL_METRICS);
            THREAD_CONTEXT.set(context);
        }

        return context;
    }

    public static void clearCurrent() {
        THREAD_CONTEXT.remove();
    }

    public static void setCurrent(final Metrics metrics) {
        clearCurrent();
        Context context = new Context(metrics);
        THREAD_CONTEXT.set(context);
    }

    public static Metrics getMetrics() {
        return getOrCreateCurrentContext().getMetrics();
    }

    @Value
    private static class Context {
        Metrics metrics;
    }
}
