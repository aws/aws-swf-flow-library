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
package com.amazonaws.services.simpleworkflow.flow.core;

import java.util.concurrent.Executor;

class TaskContext extends AsyncContextBase implements AsyncParentContext {

    private final Task task;

    private boolean executionStartedOrCompleted;

    private final String parentTaskMethodName;

    private final boolean hideStartFromMethod;

    public TaskContext(Task task, Boolean daemon, Promise<?>[] waitFor) {
        super(daemon, waitFor, 7);
        this.task = task;
        this.parentTaskMethodName = null;
        this.hideStartFromMethod = false;
    }

    public TaskContext(AsyncParentContext parent, Task task, Boolean daemon, Promise<?>[] waitFor) {
        super(parent, daemon, waitFor, 5);
        this.task = task;
        this.parentTaskMethodName = null;
        this.hideStartFromMethod = false;
    }

    public TaskContext(AsyncParentContext parent, Task task, Boolean daemon, String parentTaskMethodName,
            boolean hideParentTaskMethodName, int skipStackLines, Promise<?>[] waitFor) {
        super(parent, daemon, waitFor, skipStackLines);
        this.task = task;
        this.parentTaskMethodName = parentTaskMethodName;
        this.hideStartFromMethod = hideParentTaskMethodName;
    }

    public TaskContext(Task task, Boolean daemon, String parentTaskMethodName, boolean hideParentTaskMethodName,
            int skipStackLines, Promise<?>[] waitFor) {
        super(daemon, waitFor, skipStackLines);
        this.task = task;
        this.parentTaskMethodName = parentTaskMethodName;
        this.hideStartFromMethod = hideParentTaskMethodName;
    }

    public void cancel(Throwable cause) {
        if (cancelRequested) {
            return;
        }
        cancelRequested = true;
        if (!executionStartedOrCompleted) {
            parent.remove(this);
        }
    }

    @Override
    public void run() {
        if (cancelRequested) {
            return;
        }
        setCurrent(this);
        Error error = null;
        try {
            executionStartedOrCompleted = true;
            task.doExecute();
            parent.remove(this);
        }
        catch (Throwable e) {
            if (e instanceof Error) {
                error = (Error) e;
            }
            else {
                if (stackTrace != null && !parent.isRethrown(e)) {
                    AsyncStackTrace merged = new AsyncStackTrace(stackTrace, e.getStackTrace(), 0);
                    merged.setStartFrom(getParentTaskMethodName());
                    merged.setHideStartFromMethod(hideStartFromMethod);
                    e.setStackTrace(merged.getStackTrace());
                }
                parent.fail(this, e);
            }
        }
        finally {
            if (error != null) {
                throw error;
            }
            setCurrent(null);
        }
    }

    @Override
    public void add(AsyncContextBase async, Promise<?> waitFor) {
        parent.add(async, waitFor);
    }

    @Override
    public void remove(AsyncContextBase async) {
        parent.remove(async);
    }

    @Override
    public void fail(AsyncContextBase async, Throwable e) {
        parent.fail(async, e);
    }

    @Override
    public Executor getExecutor() {
        return parent.getExecutor();
    }

    @Override
    public boolean isRethrown(Throwable e) {
        return parent.isRethrown(e);
    }

    @Override
    public AsyncParentContext getCurrentTryCatchFinallyContext() {
        return parent;
    }

    @Override
    public boolean getDaemonFlagForHeir() {
        return isDaemon();
    }

    @Override
    public String getParentTaskMethodName() {
        return parentTaskMethodName == null ? "doExecute" : parentTaskMethodName;
    }

    @Override
    public boolean getHideStartFromMethod() {
        return hideStartFromMethod;
    }

    public String toString() {
        if (stackTrace != null) {
            return stackTrace.toString();
        }
        return super.toString();
    }
}
