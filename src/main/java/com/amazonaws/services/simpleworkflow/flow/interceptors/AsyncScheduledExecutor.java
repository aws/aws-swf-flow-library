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
package com.amazonaws.services.simpleworkflow.flow.interceptors;

import java.util.Date;

import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.core.TryFinally;

/**
 * AsyncExecutor implementation that executes commands according to a provided
 * schedule. Commands are expected to contain only non blocking asynchronous
 * code.
 * 
 * @author fateev
 */
public class AsyncScheduledExecutor implements AsyncExecutor {

    private final InvocationSchedule schedule;

    private final WorkflowClock clock;

    public AsyncScheduledExecutor(InvocationSchedule schedule, WorkflowClock clock) {
        this.schedule = schedule;
        this.clock = clock;
    }

    public void execute(AsyncRunnable command) {
        scheduleNext(command, new Date(clock.currentTimeMillis()), 0, Promise.asPromise((Date) null));
    }

    private void scheduleNext(final AsyncRunnable command, Date startTime, int pastInvocationsCount, final Promise<Date> invoked) {
        Date currentTime = new Date(clock.currentTimeMillis());
        long nextInvocationDelay = schedule.nextInvocationDelaySeconds(currentTime, startTime, invoked.get(),
                pastInvocationsCount);
        if (nextInvocationDelay >= 0) {
            Promise<Void> nextInvocationTimer = clock.createTimer(nextInvocationDelay);
            // Recursing from task (or @Asynchronous) is always OK
            executeAccordingToSchedule(command, startTime, pastInvocationsCount, nextInvocationTimer);
        }
    }

    private void executeAccordingToSchedule(final AsyncRunnable command, final Date startTime, final int pastInvocationsCount,
            Promise<Void> nextInvocationTimer) {
        final Settable<Date> invoked = new Settable<Date>();
        new TryFinally(nextInvocationTimer) {

            private Date lastInvocationTime;

            @Override
            protected void doTry() throws Throwable {
                lastInvocationTime = new Date(clock.currentTimeMillis());
                command.run();
            }

            @Override
            protected void doFinally() throws Throwable {
                // It is common mistake to recurse from doFinally or doCatch.
                // As code in doFinally and in doCatch is non cancelable it 
                // makes the whole branch non cancelable which is usually 
                // not intended. 
                invoked.set(lastInvocationTime);
            }
        };
        new Task(invoked) {

            @Override
            protected void doExecute() throws Throwable {
                scheduleNext(command, startTime, pastInvocationsCount + 1, invoked);
            }
        };
    }
}
