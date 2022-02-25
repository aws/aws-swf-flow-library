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
package com.amazonaws.services.simpleworkflow.flow.core;

import java.util.ArrayList;
import java.util.List;

abstract class AsyncContextBase implements Runnable, AsyncParentContext {

    private final static ThreadLocal<AsyncParentContext> currentContext = new ThreadLocal<AsyncParentContext>();

    static AsyncParentContext current() {
        AsyncParentContext result = currentContext.get();
        if (result == null) {
            throw new IllegalStateException("Attempt to execute asynchronous code outside of AsyncScope.doAsync() method");
        }
        return result;
    }

    static void setCurrent(AsyncParentContext newCurrent) {
        currentContext.set(newCurrent);
    }

    private final boolean daemon;

    protected final AsyncParentContext parent;

    protected AsyncStackTrace stackTrace;

    private final Promise<?>[] waitFor;
    
    private String name;

    protected boolean cancelRequested;

    public AsyncContextBase(Boolean daemon, Promise<?>[] waitFor, int skipStackLines) {
        this(current(), daemon, waitFor, skipStackLines);
    }

    public AsyncContextBase(AsyncParentContext parent, Boolean daemon, Promise<?>[] waitFor, int skipStackLines) {
        this.parent = parent;
        this.daemon = daemon == null ? parent.getDaemonFlagForHeir() : daemon;
        this.waitFor = waitFor;
        this.name = parent == null ? null : parent.getName();
        AsyncStackTrace parentStack = parent.getStackTrace();
        if (parentStack != null) {
            StackTraceElement[] stacktrace = System.getProperty("com.amazonaws.simpleworkflow.disableAsyncStackTrace", "false").equalsIgnoreCase("true") ?
                    new StackTraceElement[0] :
                    Thread.currentThread().getStackTrace();
            stackTrace = new AsyncStackTrace(parentStack, stacktrace, skipStackLines);
            stackTrace.setStartFrom(parent.getParentTaskMethodName());
            stackTrace.setHideStartFromMethod(parent.getHideStartFromMethod());
        }
        this.cancelRequested = parent.isCancelRequested();
        if (!this.cancelRequested) {
            this.parent.add(this, waitFor == null || waitFor.length == 0 ? null : new AndPromise(waitFor));
        }
    }

    public boolean isDaemon() {
        return daemon;
    }

    @Override
    public boolean isCancelRequested() {
        return cancelRequested;
    }

    public AsyncStackTrace getStackTrace() {
        return stackTrace;
    }

    public AsyncTaskInfo getTaskInfo() {
        return new AsyncTaskInfo(name, stackTrace == null ? null : stackTrace.getStackTrace(), daemon, waitFor);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @param cause
     *            the cancellation cause. Can be <code>null</code>
     */
    public abstract void cancel(Throwable cause);

    public String getAsynchronousStackTraceDumpAsString() {
        List<AsyncTaskInfo> infos = new ArrayList<AsyncTaskInfo>();
        getAsynchronousStackTraceDump(infos);
        StringBuffer sb = new StringBuffer();
        for (int j = 0; j < infos.size(); j++) {
            AsyncTaskInfo info = infos.get(j);
            if (j > 0) {
                sb.append("-----------------------------------------------------\n");
            }
            sb.append(info);
        }
        return sb.toString();
    }

    @Override
    public boolean getHideStartFromMethod() {
        return false;
    }

    protected void getAsynchronousStackTraceDump(List<AsyncTaskInfo> result) {
        result.add(getTaskInfo());
    }

}
