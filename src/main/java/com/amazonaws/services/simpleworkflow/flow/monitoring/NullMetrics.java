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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class NullMetrics implements Metrics {

    private static final NullMetrics INSTANCE = new NullMetrics();

    public static NullMetrics getInstance() {
        return INSTANCE;
    }

    @Override
    public Metrics newMetrics() {
        return new NullMetrics();
    }

    @Override
    public void addProperty(String name, String value) {
    }

    @Override
    public void recordCount(String name, double amount) {
    }

    @Override
    public void recordCount(String name, double amount, Map<String, String> dimensions) {
    }

    @Override
    public void recordCount(String name, boolean success) {
    }

    @Override
    public void recordCount(String name, boolean success, Map<String, String> dimensions) {
    }

    @Override
    public void record(String name, long amount, TimeUnit unit) {
    }

    @Override
    public void record(String name, long amount, TimeUnit unit, Map<String, String> dimensions) {
    }

    @Override
    public <T> T recordSupplier(Supplier<T> supplier, String operation, TimeUnit unit) {
        return supplier.get();
    }

    @Override
    public void recordRunnable(Runnable runnable, String operation, TimeUnit unit) {
        runnable.run();
    }

    @Override
    public <T> T recordCallable(Callable<T> callable, String operation, TimeUnit unit) throws Exception {
        return callable.call();
    }

    @Override
    public void close() {
    }
}
