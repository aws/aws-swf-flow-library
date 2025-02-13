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
package com.amazonaws.services.simpleworkflow.flow.interceptors;

import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.core.TryCatchFinally;

/**
 * Retries failed command according to the specified retryPolicy.
 * 
 * @author fateev
 */
public class AsyncRetryingExecutor implements AsyncExecutor {

    private final RetryPolicy retryPolicy;

    private WorkflowClock clock;

    private AtomicLong firstAttemptTime;

    public AsyncRetryingExecutor(RetryPolicy retryPolicy, WorkflowClock clock, AtomicLong firstAttemptTime) {
        this.retryPolicy = retryPolicy;
        this.clock = clock;
        this.firstAttemptTime = firstAttemptTime;
    }

    public AsyncRetryingExecutor(RetryPolicy retryPolicy, WorkflowClock clock) {
        this(retryPolicy, clock, new AtomicLong(0));
    }

    @Override
    public void execute(final AsyncRunnable command) {
        // Task is used only to avoid wrapping Throwable thrown from scheduleWithRetry
        new Task() {
            @Override
            protected void doExecute() throws Throwable {
                scheduleWithRetry(command, null, 1, clock.currentTimeMillis(), 0);
            }
        };
    }

    private void scheduleWithRetry(final AsyncRunnable command, final Throwable failure,
                                   final int attempt, final long timeOfFirstAttempt, final long timeOfRecordedFailure) throws Throwable {
        long delay = -1;
        if (attempt > 1) {
            if (!retryPolicy.isRetryable(failure)) {
                throw failure;
            }
            Date firstAttempt;
            if (firstAttemptTime.get() == 0) {
                firstAttempt = new Date(timeOfFirstAttempt);
            } else {
                firstAttempt = new Date(firstAttemptTime.get());
            }
            delay = retryPolicy.nextRetryDelaySeconds(firstAttempt, new Date(timeOfRecordedFailure), attempt);
            if (delay < 0) {
                throw failure;
            }
        }

        if (delay > 0) {
            Promise<Void> timer = clock.createTimer(delay);
            new Task(timer) {

                @Override
                protected void doExecute() throws Throwable {
                    invoke(command, attempt, timeOfFirstAttempt);
                }
            };
        }
        else {
            invoke(command, attempt, timeOfFirstAttempt);
        }

    }

    private void invoke(final AsyncRunnable command, final int attempt, final long timeOfFirstAttempt) {
        final Settable<Throwable> shouldRetry = new Settable<Throwable>();

        new TryCatchFinally() {

            Throwable failureToRetry = null;

            @Override
            protected void doTry() throws Throwable {
                command.run();
            }

            @Override
            protected void doCatch(Throwable failure) throws Throwable {
                if (failure instanceof CancellationException) {
                    throw failure;
                }
                failureToRetry = failure;
            }

            @Override
            protected void doFinally() throws Throwable {
                shouldRetry.set(failureToRetry);
            }
        };

        new Task(shouldRetry) {

            @Override
            protected void doExecute() throws Throwable {
                Throwable failure = shouldRetry.get();
                if (failure != null) {
                    scheduleWithRetry(command, failure, attempt + 1, timeOfFirstAttempt, clock.currentTimeMillis());
                }
            }
        };
    }

}
