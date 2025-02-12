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

import static com.amazonaws.services.simpleworkflow.flow.model.ActivityType.fromSdkType;

import com.amazonaws.services.simpleworkflow.flow.ActivityTaskFailedException;
import com.amazonaws.services.simpleworkflow.flow.ActivityTaskTimedOutException;
import com.amazonaws.services.simpleworkflow.flow.ScheduleActivityTaskFailedException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTask;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTaskCancellationHandler;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTaskCompletionHandle;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.generic.ExecuteActivityParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericActivityClient;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import software.amazon.awssdk.services.swf.model.ActivityTaskCanceledEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskTimedOutEventAttributes;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.TaskList;

class GenericActivityClientImpl implements GenericActivityClient {

    private final class ActivityCancellationHandler implements ExternalTaskCancellationHandler {

        private final String activityId;

        private final ExternalTaskCompletionHandle handle;

        private ActivityCancellationHandler(String activityId, ExternalTaskCompletionHandle handle) {
            this.activityId = activityId;
            this.handle = handle;
        }

        @Override
        public void handleCancellation(Throwable cause) {
            decisions.requestCancelActivityTask(activityId, new Runnable() {

                @Override
                public void run() {
                    OpenRequestInfo<String, ActivityType> scheduled = scheduledActivities.remove(activityId);
                    if (scheduled == null) {
                        throw new IllegalArgumentException("Activity \"" + activityId + "\" wasn't scheduled");
                    }
                    handle.complete();
                }
            });
        }
    }

    private final DecisionsHelper decisions;

    private final Map<String, OpenRequestInfo<String, ActivityType>> scheduledActivities = new HashMap<String, OpenRequestInfo<String, ActivityType>>();

    public GenericActivityClientImpl(DecisionsHelper decisions) {
        this.decisions = decisions;
    }

    @Override
    public Promise<String> scheduleActivityTask(final ExecuteActivityParameters parameters) {
        final OpenRequestInfo<String, ActivityType> context = new OpenRequestInfo<String, ActivityType>(
                parameters.getActivityType());
        ScheduleActivityTaskDecisionAttributes.Builder attributeBuilder = ScheduleActivityTaskDecisionAttributes.builder()
            .activityType(parameters.getActivityType().toSdkType())
            .input(parameters.getInput())
            .heartbeatTimeout(FlowHelpers.secondsToDuration(parameters.getHeartbeatTimeoutSeconds()))
            .scheduleToCloseTimeout(FlowHelpers.secondsToDuration(parameters.getScheduleToCloseTimeoutSeconds()))
            .scheduleToStartTimeout(FlowHelpers.secondsToDuration(parameters.getScheduleToStartTimeoutSeconds()))
            .startToCloseTimeout(FlowHelpers.secondsToDuration(parameters.getStartToCloseTimeoutSeconds()))
            .taskPriority(FlowHelpers.taskPriorityToString(parameters.getTaskPriority())).control(parameters.getControl());
        String activityId = parameters.getActivityId();
        if (activityId == null) {
            activityId = String.valueOf(decisions.getNextId());
        }
        attributeBuilder.activityId(activityId);
        String taskList = parameters.getTaskList();
        if (taskList != null && !taskList.isEmpty()) {
            attributeBuilder.taskList(TaskList.builder().name(taskList).build());
        }
        final ScheduleActivityTaskDecisionAttributes attributes = attributeBuilder.build();
        String taskName = "activityId=" + activityId + ", activityType=" + attributes.activityType();
        new ExternalTask() {

            @Override
            protected ExternalTaskCancellationHandler doExecute(final ExternalTaskCompletionHandle handle) throws Throwable {

                decisions.scheduleActivityTask(attributes);
                context.setCompletionHandle(handle);
                scheduledActivities.put(attributes.activityId(), context);
                return new ActivityCancellationHandler(attributes.activityId(), handle);
            }
        }.setName(taskName);
        context.setResultDescription("scheduleActivityTask " + taskName);
        return context.getResult();
    }

    @Override
    public Promise<String> scheduleActivityTask(final String activity, final String version, final Promise<String> input) {
        final Settable<String> result = new Settable<String>();
        new Task(input) {

            @Override
            protected void doExecute() throws Throwable {
                result.chain(scheduleActivityTask(activity, version, input.get()));
            }
        };
        return result;
    }

    @Override
    public Promise<String> scheduleActivityTask(String activity, String version, String input) {
        ExecuteActivityParameters parameters = new ExecuteActivityParameters();
        parameters.setActivityType(ActivityType.builder().name(activity).version(version).build());
        parameters.setInput(input);
        return scheduleActivityTask(parameters);
    }

    void handleActivityTaskStarted(ActivityTaskStartedEventAttributes attributes) {
    }

    void handleActivityTaskCanceled(HistoryEvent event) {
        ActivityTaskCanceledEventAttributes attributes = event.activityTaskCanceledEventAttributes();
        String activityId = decisions.getActivityId(attributes);
        if (decisions.handleActivityTaskCanceled(event)) {
            CancellationException e = new CancellationException();
            OpenRequestInfo<String, ActivityType> scheduled = scheduledActivities.remove(activityId);
            if (scheduled != null) {
                ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
                // It is OK to fail with subclass of CancellationException when cancellation requested.
                // It allows passing information about cancellation (details in this case) to the surrounding doCatch block
                completionHandle.fail(e);
            }
        }
    }

    void handleScheduleActivityTaskFailed(HistoryEvent event) {
        ScheduleActivityTaskFailedEventAttributes attributes = event.scheduleActivityTaskFailedEventAttributes();
        String activityId = attributes.activityId();
        OpenRequestInfo<String, ActivityType> scheduled = scheduledActivities.remove(activityId);
        if (decisions.handleScheduleActivityTaskFailed(event)) {
            String cause = attributes.causeAsString();
            ScheduleActivityTaskFailedException failure = new ScheduleActivityTaskFailedException(event.eventId(),
                fromSdkType(attributes.activityType()), activityId, cause);
            ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
            completionHandle.fail(failure);
        }
    }

    void handleActivityTaskCompleted(HistoryEvent event) {
        ActivityTaskCompletedEventAttributes attributes = event.activityTaskCompletedEventAttributes();
        String activityId = decisions.getActivityId(attributes);
        if (decisions.handleActivityTaskClosed(activityId)) {
            OpenRequestInfo<String, ActivityType> scheduled = scheduledActivities.remove(activityId);
            if (scheduled != null) {
                String result = attributes.result();
                scheduled.getResult().set(result);
                ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
                completionHandle.complete();
            }
        }
    }

    void handleActivityTaskFailed(HistoryEvent event) {
        ActivityTaskFailedEventAttributes attributes = event.activityTaskFailedEventAttributes();
        String activityId = decisions.getActivityId(attributes);
        if (decisions.handleActivityTaskClosed(activityId)) {
            OpenRequestInfo<String, ActivityType> scheduled = scheduledActivities.remove(activityId);
            if (scheduled != null) {
                String reason = attributes.reason();
                String details = attributes.details();
                ActivityTaskFailedException failure = new ActivityTaskFailedException(event.eventId(),
                        scheduled.getUserContext(), activityId, reason, details);
                ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
                completionHandle.fail(failure);
            }
        }
    }

    void handleActivityTaskTimedOut(HistoryEvent event) {
        ActivityTaskTimedOutEventAttributes attributes = event.activityTaskTimedOutEventAttributes();
        String activityId = decisions.getActivityId(attributes);
        if (decisions.handleActivityTaskClosed(activityId)) {
            OpenRequestInfo<String, ActivityType> scheduled = scheduledActivities.remove(activityId);
            if (scheduled != null) {
                String timeoutType = attributes.timeoutTypeAsString();
                String details = attributes.details();
                ActivityTaskTimedOutException failure = new ActivityTaskTimedOutException(event.eventId(),
                        scheduled.getUserContext(), activityId, timeoutType, details);
                ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
                completionHandle.fail(failure);
            }
        }
    }

}
