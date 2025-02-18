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
package com.amazonaws.services.simpleworkflow.flow.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.simpleworkflow.flow.StartTimerFailedException;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTask;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTaskCancellationHandler;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTaskCompletionHandle;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.StartTimerDecisionAttributes;
import software.amazon.awssdk.services.swf.model.StartTimerFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.TimerCanceledEventAttributes;
import software.amazon.awssdk.services.swf.model.TimerFiredEventAttributes;

class WorkflowClockImpl implements WorkflowClock {

    private static final Log log = LogFactory.getLog(WorkflowClockImpl.class);

    private final class TimerCancellationHandler implements ExternalTaskCancellationHandler {

        private final String timerId;

        private TimerCancellationHandler(String timerId) {
            this.timerId = timerId;
        }

        @Override
        public void handleCancellation(Throwable cause) {
            decisions.cancelTimer(timerId, new Runnable() {

                @Override
                public void run() {
                    OpenRequestInfo<?, ?> scheduled = scheduledTimers.remove(timerId);
                    ExternalTaskCompletionHandle context = scheduled.getCompletionHandle();
                    context.complete();
                }
            });
        }
    }

    private final DecisionsHelper decisions;

    private final Map<String, OpenRequestInfo<?, ?>> scheduledTimers = new HashMap<String, OpenRequestInfo<?, ?>>();

    private long replayCurrentTimeMilliseconds;

    private boolean replaying = true;

    WorkflowClockImpl(DecisionsHelper decisions) {
        this.decisions = decisions;
    }

    @Override
    public long currentTimeMillis() {
        return replayCurrentTimeMilliseconds;
    }

    void setReplayCurrentTimeMilliseconds(long replayCurrentTimeMilliseconds) {
        this.replayCurrentTimeMilliseconds = replayCurrentTimeMilliseconds;
    }

    @Override
    public boolean isReplaying() {
        return replaying;
    }

    void setReplaying(boolean replaying) {
        this.replaying = replaying;
    }

    @Override
    public Promise<Void> createTimer(long delaySeconds) {
        return createTimer(delaySeconds, null);
    }
    
    @Override
    public <T> Promise<T> createTimer(final long delaySeconds, final T userContext) {
    	final String timerId = decisions.getNextId();
    	return createTimer(delaySeconds, userContext, timerId);
    }

    @Override
    public <T> Promise<T> createTimer(final long delaySeconds, final T userContext, final String timerId) {
        if (delaySeconds < 0) {
            throw new IllegalArgumentException("Negative delaySeconds: " + delaySeconds);
        }
        if (delaySeconds == 0) {
            return Promise.asPromise(userContext);
        }
        final OpenRequestInfo<T, Object> context = new OpenRequestInfo<T, Object>(userContext);
        final StartTimerDecisionAttributes timer = StartTimerDecisionAttributes.builder()
            .startToFireTimeout(FlowHelpers.secondsToDuration(delaySeconds)).timerId(timerId).build();
        String taskName = "timerId=" + timer.timerId() + ", delaySeconds=" + timer.startToFireTimeout();
        new ExternalTask() {

            @Override
            protected ExternalTaskCancellationHandler doExecute(ExternalTaskCompletionHandle handle) throws Throwable {

                decisions.startTimer(timer, userContext);
                context.setCompletionHandle(handle);
                scheduledTimers.put(timerId, context);
                return new TimerCancellationHandler(timerId);
            }
        }.setName(taskName);
        context.setResultDescription("createTimer " + taskName);
        return context.getResult();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    void handleTimerFired(Long eventId, TimerFiredEventAttributes attributes) {
        String timerId = attributes.timerId();
        if (decisions.handleTimerClosed(timerId)) {
            OpenRequestInfo scheduled = scheduledTimers.remove(timerId);
            if (scheduled != null) {
                ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
                scheduled.getResult().set(scheduled.getUserContext());
                completionHandle.complete();
            }
        }
        else {
            log.debug("handleTimerFired not complete");
        }
    }

    @SuppressWarnings({ "rawtypes" })
    void handleStartTimerFailed(HistoryEvent event) {
        StartTimerFailedEventAttributes attributes = event.startTimerFailedEventAttributes();
        String timerId = attributes.timerId();
        if (decisions.handleStartTimerFailed(event)) {
            OpenRequestInfo scheduled = scheduledTimers.remove(timerId);
            if (scheduled != null) {
                ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
                Object createTimerUserContext = scheduled.getUserContext();
                String cause = attributes.causeAsString();
                Throwable failure = new StartTimerFailedException(event.eventId(), timerId, createTimerUserContext, cause);
                completionHandle.fail(failure);
            }
        }
        else {
            log.debug("handleStartTimerFailed not complete");
        }
    }

    void handleTimerCanceled(HistoryEvent event) {
        TimerCanceledEventAttributes attributes = event.timerCanceledEventAttributes();
        String timerId = attributes.timerId();
        if (decisions.handleTimerCanceled(event)) {
            OpenRequestInfo<?, ?> scheduled = scheduledTimers.remove(timerId);
            if (scheduled != null) {
                ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
                CancellationException exception = new CancellationException();
                completionHandle.fail(exception);
            }
        }
        else {
            log.debug("handleTimerCanceled not complete");
        }
    }

}
