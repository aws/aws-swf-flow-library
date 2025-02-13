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
package com.amazonaws.services.simpleworkflow.flow.monitoring;

import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import java.util.Collections;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum MetricName {
    LATENCY("Latency"),
    EMPTY_POLL_COUNT("EmptyPollCount"),
    PAGE_COUNT("PageCount"),
    TYPE_NOT_FOUND("TypeNotFound"),
    RESPONSE_TRUNCATED("ResponseTruncated"),
    DROPPED_TASK("TaskDropped"),
    FAIL_WORKFLOW_UNEXPECTED_ERROR("FailWorkflowUnexpectedError"),
    DECISION_COUNT("DecisionCount"),
    DECISION_LIST_TRUNCATED("DecisionListTruncated"),
    OUTSTANDING_TASKS_DROPPED("OutstandingTasksDropped"),
    ACTIVITY_CANCEL_REQUESTED("ActivityCancelRequested"),
    VALID_DECIDER_FOUND_IN_CACHE("ValidDeciderFoundInCache"),
    AFFINITY_WORKER_START_FAILURE("AffinityWorkerStartFailure"),
    AFFINITY_WORKER_WORKFLOW_EXECUTION_LOCAL_FAILURE("AffinityWorkerWorkflowExecutionLocalFailure"),
    AFFINITY_FULL_HISTORY_FORCE_FETCHED("AffinityFullHistoryForceFetched"),
    AFFINITY_FULL_HISTORY_FORCE_FETCH_FAILURE("AffinityFullHistoryForceFetchFailure"),
    SUCCESSIVE_DECISION_FAILURE("SuccessiveDecisionFailure"),
    REQUEST_SIZE_MAY_BE_EXCEEDED("RequestSizeMayBeExceeded"),
    MAXIMUM_HISTORY_EVENT_ID("MaxHistoryEventId")
    ;

    @Getter
    private final String name;

    public static Map<String, String> getTaskListDimension(final String taskList) {
        return Collections.singletonMap(DimensionKey.TASK_LIST.getKey(), taskList);
    }

    public static Map<String, String> getOperationDimension(final String operation) {
        return Collections.singletonMap(DimensionKey.OPERATION.getKey(), operation);
    }

    public static Map<String, String> getResultDimension(final boolean success) {
        return Collections.singletonMap(DimensionKey.RESULT.getKey(), success ? "Success" : "Fail");
    }

    public static Map<String, String> getActivityTypeDimension(final ActivityType type) {
        return Collections.singletonMap(DimensionKey.ACTIVITY_TYPE.getKey(), type.getName() + "@" + type.getVersion());
    }

    public static Map<String, String> getWorkflowTypeDimension(final WorkflowType type) {
        return Collections.singletonMap(DimensionKey.WORKFLOW_TYPE.getKey(), type.getName() + "@" + type.getVersion());
    }

    @RequiredArgsConstructor
    public enum DimensionKey {
        OPERATION("Operation"),
        RESULT("Result"),
        TASK_LIST("TaskList"),
        ACTIVITY_TYPE("ActivityType"),
        WORKFLOW_TYPE("WorkflowType"),
        ;

        @Getter
        private final String key;
    }

    @RequiredArgsConstructor
    public enum Operation {
        DECISION_TASK_POLL("DecisionTaskPoll"),
        ACTIVITY_TASK_POLL("ActivityTaskPoll"),
        EXECUTE_DECISION_TASK("DecisionTaskExecution"),
        EXECUTE_ACTIVITY_TASK("ActivityTaskExecution"),
        REGISTER_ACTIVITY_TYPE("RegisterActivityType"),
        REGISTER_WORKFLOW_TYPE("RegisterWorkflowType"),
        POLL_FOR_DECISION_TASK("PollForDecisionTask"),
        POLL_FOR_ACTIVITY_TASK("PollForActivityTask"),
        RESPOND_DECISION_TASK_COMPLETED("RespondDecisionTaskCompleted"),
        RESPOND_ACTIVITY_TASK_COMPLETED("RespondActivityTaskCompleted"),
        RESPOND_ACTIVITY_TASK_FAILED("RespondActivityTaskFailed"),
        RESPOND_ACTIVITY_TASK_CANCELED("RespondActivityTaskCanceled"),
        RECORD_ACTIVITY_TASK_HEARTBEAT("RecordActivityTaskHeartbeat"),
        GET_WORKFLOW_EXECUTION_HISTORY("GetWorkflowExecutionHistory"),
        DATA_CONVERTER_SERIALIZE("DataConverter@Serialize"),
        DATA_CONVERTER_DESERIALIZE("DataConverter@Deserialize"),
        ACQUIRE_ACTIVITY_POLL_PERMIT("AcquireActivityPollPermit"),
        ACQUIRE_DECIDER_POLL_PERMIT("AcquireDeciderPollPermit"),
        WORKFLOW_WORKER_SHUTDOWN("WorkflowWorkerShutdown"),
        ACTIVITY_WORKER_SHUTDOWN("ActivityWorkerShutdown")
        ;

        @Getter
        private final String name;
    }

    @RequiredArgsConstructor
    public enum Property {
        SDK_INVOCATION_ID("amz-sdk-invocation-id"),
        SDK_RETRY_INFO("amz-sdk-retry"),
        REQUEST_IDS("x-amzn-RequestId"),
        STATUS_CODE("StatusCode"),
        WORKFLOW_ID("WorkflowId"),
        RUN_ID("RunId"),
        TASK_STARTED_EVENT_ID("TaskStartedEventId"),
        TASK_TOKEN("TaskToken"),
        PARENT_WORKFLOW_ID("ParentWorkflowId"),
        PARENT_RUN_ID("ParentRunId"),
        DOMAIN("Domain"),
        TASK_LIST("TaskList"),
        ACTIVITY_TYPE("ActivityType"),
        ACTIVITY_TYPE_NAME("ActivityTypeName"),
        ACTIVITY_TYPE_VERSION("ActivityTypeVersion"),
        WORKFLOW_TYPE("WorkflowType"),
        WORKFLOW_TYPE_NAME("WorkflowTypeName"),
        WORKFLOW_TYPE_VERSION("WorkflowTypeVersion"),
        START_TIME("StartTime"),
        END_TIME("EndTime"),
        TIME("Time")
        ;

        @Getter
        private final String name;
    }
}
