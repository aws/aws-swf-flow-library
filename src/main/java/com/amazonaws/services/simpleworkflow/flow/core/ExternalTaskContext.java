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

class ExternalTaskContext extends AsyncContextBase {

    private final class ExternalTaskCompletionHandleImpl implements ExternalTaskCompletionHandle {

        private String methodName;

        private boolean completed;

        private Throwable failure;

        private void setDoExecuteFailed(String methodName, Throwable e) {
            this.failure = e;
            this.methodName = methodName;
        }

        @Override
        public void complete() {
            if (failure != null) {
                throw new IllegalStateException("Invalid ExternalTaskCompletionHandle as " + methodName
                        + " failed with an exception.", failure);
            }
            if (completed) {
                throw new IllegalStateException("Already completed");
            }
            completed = true;
            if (!inCancellationHandler) {
                removeFromParent();
            }
        }

        @Override
        public void fail(final Throwable e) {
            if (e instanceof Error) {
                throw (Error) e;
            }
            if (failure != null) {
                throw new IllegalStateException("Invalid ExternalTaskCompletionHandle as " + methodName
                        + " failed with exception.", failure);
            }
            if (completed) {
                throw new IllegalStateException("Already completed");
            }
            if (stackTrace != null && !parent.isRethrown(e)) {
                AsyncStackTrace merged = new AsyncStackTrace(stackTrace, e.getStackTrace(), 0);
                merged.setStartFrom(getParentTaskMethodName());
                e.setStackTrace(merged.getStackTrace());
            }
            failure = e;
            if (!inCancellationHandler) {
                failToParent(e);
            }
        }

        public boolean isCompleted() {
            return completed;
        }

        public Throwable getFailure() {
            return failure;
        }

    }

    private final ExternalTask task;

    private ExternalTaskCancellationHandler cancellationHandler;

    /**
     * Used to deal with situation when task is completed while in cancellation
     * handler and then exception is thrown from it.
     */
    private boolean inCancellationHandler;

    private ExternalTaskCompletionHandleImpl completionHandle = new ExternalTaskCompletionHandleImpl();

    private String description;

    public ExternalTaskContext(ExternalTask task, Boolean daemon, Promise<?>[] waitFor) {
        super(daemon, waitFor, 6);
        this.task = task;
    }

    public ExternalTaskContext(AsyncParentContext parent, ExternalTask task, Boolean daemon, Promise<?>[] waitFor) {
        super(parent, daemon, waitFor, 6);
        this.task = task;
    }

    public void cancel(final Throwable cause) {
        if (completionHandle.failure != null || completionHandle.completed) {
            return;
        }
        if (cancelRequested) {
            return;
        }
        cancelRequested = true;
        if (cancellationHandler != null) {
            parent.getExecutor().execute(new Runnable() {

                @Override
                public void run() {
                    Error error = null;
                    try {
                        inCancellationHandler = true;
                        cancellationHandler.handleCancellation(cause);
                    }
                    catch (Throwable e) {
                        if (e instanceof Error) {
                            error = (Error) e;
                        }
                        else {
                            if (stackTrace != null && !parent.isRethrown(e)) {
                                AsyncStackTrace merged = new AsyncStackTrace(stackTrace, e.getStackTrace(), 0);
                                merged.setStartFrom(getParentTaskMethodName());
                                e.setStackTrace(merged.getStackTrace());
                            }
                            completionHandle.setDoExecuteFailed("ExternalTaskCancellationHandler.handleCancellation", e);
                        }
                    }
                    finally {
                        if (error != null) {
                            throw error;
                        }
                        inCancellationHandler = false;
                        if (completionHandle.getFailure() != null) {
                            failToParent(completionHandle.getFailure());
                        }
                        else if (completionHandle.isCompleted()) {
                            removeFromParent();
                        }
                    }
                }
            });
        } else {
            removeFromParent();
        }
    }

    @Override
    public void run() {
        if (cancelRequested) {
            return;
        }
        setCurrent(parent);
        try {
            cancellationHandler = task.doExecute(completionHandle);
        }
        catch (Throwable e) {
            completionHandle.setDoExecuteFailed("ExternalTask.doExecute", e);
            if (stackTrace != null && !parent.isRethrown(e)) {
                AsyncStackTrace merged = new AsyncStackTrace(stackTrace, e.getStackTrace(), 0);
                merged.setStartFrom(getParentTaskMethodName());
                e.setStackTrace(merged.getStackTrace());
            }
            parent.fail(this, e);
        }
        finally {
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
        if (cancelRequested) {
            return "handleCancellation";
        }
        return "doExecute";
    }

    private void removeFromParent() {
        parent.getExecutor().execute(new Runnable() {

            @Override
            public void run() {
                parent.remove(ExternalTaskContext.this);
            }
        });
    }

    private void failToParent(final Throwable e) {
        parent.getExecutor().execute(new Runnable() {

            @Override
            public void run() {
                parent.fail(ExternalTaskContext.this, e);
            }
        });
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public String toString() {
        if (stackTrace != null) {
            return stackTrace.toString();
        }
        return super.toString();
    }
    
}
