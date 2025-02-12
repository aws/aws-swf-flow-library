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
package com.amazonaws.services.simpleworkflow.flow;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Contains value that is bound to a currently executing workflow. Has the same
 * purpose as {@link ThreadLocal} which bounds value to a particular thread. It
 * is subject to the same replay rules as the rest of the workflow definition.
 */
public class WorkflowExecutionLocal<T> {

    public static class Wrapper<T> {

        public T wrapped;
    }

    /**
     * It is not good idea to rely on the fact that implementation relies on
     * ThreadLocal as it is subject to change.
     */
    private final ThreadLocal<Wrapper<T>> value = new ThreadLocal<>();

    private final String workflowExecutionLocalId = UUID.randomUUID().toString();

    private final static Map<String, WorkflowExecutionLocal<?>> locals = new HashMap<>();

    /**
     * Must be called before each decision. It is not a good idea to call this
     * method from non framework code for non testing scenarios.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void before() {
        Map<String, WorkflowExecutionLocal<?>> currentLocals;
        synchronized (locals) {
            currentLocals = new HashMap<>(locals);
        }
        for (WorkflowExecutionLocal local : currentLocals.values()) {
            Wrapper w = new Wrapper();
            w.wrapped = local.initialValue();
            local.set(w);
        }
    }

    /**
     * Must be called at the end of each decision. It is not a good idea to call
     * this method from non framework code for non testing scenarios.
     */
    public static void after() {
        Map<String, WorkflowExecutionLocal<?>> currentLocals;
        synchronized (locals) {
            currentLocals = new HashMap<>(locals);
        }
        for (WorkflowExecutionLocal<?> local : currentLocals.values()) {
            local.removeAfter();
        }
    }

    public static Map<String, Wrapper> saveCurrentValues() {
        Map<String, WorkflowExecutionLocal<?>> currentLocals;
        synchronized (locals) {
            currentLocals = new HashMap<>(locals);
        }
        Map<String, Wrapper> currentValues = new HashMap<>();
        currentLocals.forEach((id, local) -> {
            if (local.value.get() != null) {
                Wrapper w = new Wrapper();
                w.wrapped = local.value.get().wrapped;
                currentValues.put(id, w);
            }
        });
        return currentValues;
    }

    public static void restoreFromSavedValues(Map<String, Wrapper> savedValues) {
        Map<String, WorkflowExecutionLocal<?>> currentLocals;
        synchronized (locals) {
            currentLocals = new HashMap<>(locals);
        }
        currentLocals.forEach((id, local) -> {
            if (savedValues.containsKey(id)) {
                local.set(savedValues.get(id));
            }
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public WorkflowExecutionLocal() {
        Wrapper w = new Wrapper();
        w.wrapped = initialValue();
        set(w);
        synchronized (locals) {
            locals.put(workflowExecutionLocalId, this);
        }
    }

    public T get() {
        Wrapper<T> w = getWrapped();
        return w.wrapped;
    }

    private Wrapper<T> getWrapped() {
        Wrapper<T> w = value.get();
        if (w == null) {
            throw new IllegalStateException("Called outside of the workflow definition code.");
        }
        return w;
    }

    public int hashCode() {
        Wrapper<T> w = getWrapped();
        return w.wrapped.hashCode();
    }

    public void remove() {
        Wrapper<T> w = getWrapped();
        w.wrapped = null;
    }

    public void set(T v) {
        Wrapper<T> w = getWrapped();
        w.wrapped = v;
    }

    private void set(Wrapper<T> w) {
        value.set(w);
    }
    
    private void removeAfter() {
        value.remove();
    }
    
    protected T initialValue() {
        return null;
    }

}
