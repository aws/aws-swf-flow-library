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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;

import com.amazonaws.services.simpleworkflow.flow.core.TryCatchFinally.State;

class TryCatchFinallyContext extends AsyncContextBase {

    private List<AsyncContextBase> heirs = new ArrayList<AsyncContextBase>();

    private int nonDaemonHeirsCount;

    private Executor executor;

    private State state = State.CREATED;

    private Throwable failure;

    private boolean executed;

    private boolean daemondCausedCancellation;

    private final TryCatchFinally tryCatchFinally;

    private final String parentTaskMethodName;

    TryCatchFinallyContext(TryCatchFinally tryCatchFinally, Boolean daemon, String parentTaskMethodName, int skipStackLines,
            Promise<?>[] waitFor) {
        super(daemon, waitFor, skipStackLines);
        this.tryCatchFinally = tryCatchFinally;
        this.executor = parent.getExecutor();
        this.parentTaskMethodName = parentTaskMethodName;
    }

    TryCatchFinallyContext(AsyncParentContext parent, TryCatchFinally tryCatchFinally, Boolean daemon,
            String parentTaskMethodName, int skipStackLines, Promise<?>[] waitFor) {
        super(parent, daemon, waitFor, skipStackLines);
        this.tryCatchFinally = tryCatchFinally;
        this.executor = parent.getExecutor();
        this.parentTaskMethodName = parentTaskMethodName;
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void add(final AsyncContextBase async, Promise<?> waitFor) {
        assert !cancelRequested;
        checkClosed();
        heirs.add(async);
        if (!async.isDaemon()) {
            nonDaemonHeirsCount++;
        }
        if (waitFor == null) {
            executor.execute(async);
        }
        else {
            waitFor.addCallback(new Runnable() {

                @Override
                public void run() {
                    executor.execute(async);
                }

            });
        }
    }

    private void checkClosed() {
        if (state == State.CLOSED) {
            throw new IllegalStateException(state.toString());
        }
    }

    public void cancel(Throwable cause) {
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cancelRequested) {
            return;
        }
        if (state == State.CREATED) {
            assert heirs.size() == 0;
            state = State.CLOSED;
            parent.remove(this);
            return;
        }
        if (failure == null && state == State.TRYING) {
            cancelRequested = true;
            failure = new CancellationException();
            if (stackTrace != null) {
                failure.setStackTrace(stackTrace.getStackTrace());
            }
            failure.initCause(cause);
            cancelHeirs();
        }
    }

    public void remove(AsyncContextBase async) {
        checkClosed();
        boolean removed = heirs.remove(async);
        assert removed;
        if (!async.isDaemon()) {
            nonDaemonHeirsCount--;
            assert nonDaemonHeirsCount >= 0;
        }
        updateState();
    }

    public void fail(AsyncContextBase async, Throwable e) {
        checkClosed();
        boolean cancellationException = e instanceof CancellationException;
        // Explicit cancellation through cancel() call leads to CancellationException being 
        // thrown from the cancelled component. At the same time cancellation caused by the 
        // daemon flag is ignored.
        if (!cancellationException || (failure == null && !daemondCausedCancellation)) {
            failure = e;
        }
        boolean removed = heirs.remove(async);
        assert removed;
        if (!async.isDaemon()) {
            nonDaemonHeirsCount--;
            assert nonDaemonHeirsCount >= 0;
        }
        cancelHeirs();
        updateState();
    }

    @Override
    public void run() {
        if (state == State.CLOSED) {
            return;
        }
        if (state == State.CREATED) {
            state = State.TRYING;
        }
        setCurrent(this);
        Throwable f = failure;
        Error error = null;
        try {
            switch (state) {
            case TRYING:
                if (cancelRequested) {
                    return;
                }
                tryCatchFinally.doTry();
                break;
            case CATCHING:
                failure = null;
                // Need to reset cancelRequested to allow addition of new child tasks
                cancelRequested = false;
                tryCatchFinally.doCatch(f);
                break;
            case FINALIZING:
                // Need to reset cancelRequested to allow addition of new child tasks
                cancelRequested = false;
                tryCatchFinally.doFinally();
            }
        }
        catch (Throwable e) {
            if (e instanceof Error) {
                error = (Error) e;
            }
            else {
                if (stackTrace != null && e != f) {
                    AsyncStackTrace merged = new AsyncStackTrace(stackTrace, e.getStackTrace(), 0);
                    merged.setStartFrom(getParentTaskMethodName());
                    e.setStackTrace(merged.getStackTrace());
                }
                failure = e;
                cancelHeirs();
            }
        }
        finally {
            if (error != null) {
                throw error;
            }
            setCurrent(null);
            executed = true;
            updateState();
        }
    }

    private void cancelHeirs() {
        List<AsyncContextBase> toCancel = new ArrayList<AsyncContextBase>(heirs);
        for (AsyncContextBase heir : toCancel) {
            heir.cancel(failure);
        }
    }

    private void updateState() {
        if (state == State.CLOSED || !executed) {
            return;
        }
        if (nonDaemonHeirsCount == 0) {
            if (heirs.isEmpty()) {
                if (state == State.TRYING) {
                    if (failure == null) {
                        state = State.FINALIZING;
                        execute();
                    }
                    else {
                        state = State.CATCHING;
                        execute();
                    }
                }
                else if (state == State.CATCHING) {
                    state = State.FINALIZING;
                    execute();
                }
                else if (state == State.FINALIZING) {
                    assert state != State.CLOSED;
                    state = State.CLOSED;
                    if (failure == null) {
                        parent.remove(this);
                    }
                    else {
                        parent.fail(this, failure);
                    }
                }
                else {
                    throw new IllegalStateException("Unknown state " + state);
                }
            }
            else {
                if (failure == null) {
                    daemondCausedCancellation = true;
                }
                cancelHeirs();
            }
        }
    }

    private void execute() {
        executed = false;
        executor.execute(this);
    }

    @Override
    protected void getAsynchronousStackTraceDump(List<AsyncTaskInfo> result) {
        if (heirs.size() == 0) {
            result.add(getTaskInfo());
        }
        else {
            for (AsyncContextBase heir : heirs) {
                heir.getAsynchronousStackTraceDump(result);
            }
        }
    }

    public boolean isRethrown(Throwable e) {
        return e == failure;
    }

    @Override
    public AsyncParentContext getCurrentTryCatchFinallyContext() {
        return this;
    }

    /**
     * Heirs of the TryCatchFinally do not inherit daemon flag.
     */
    @Override
    public boolean getDaemonFlagForHeir() {
        return false;
    }

    @Override
    public String getParentTaskMethodName() {
        if (parentTaskMethodName != null) {
            return parentTaskMethodName;
        }
        if (state == State.TRYING) {
            return "doTry";
        }
        if (state == State.CATCHING) {
            return "doCatch";
        }
        if (state == State.FINALIZING) {
            return "doFinally";
        }
        return null;
    }

    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        if (stackTrace != null) {
            return stackTrace.toString();
        }
        return super.toString();
    }

}
