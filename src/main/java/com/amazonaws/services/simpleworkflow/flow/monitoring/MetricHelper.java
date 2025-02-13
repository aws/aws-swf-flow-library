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

import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution.fromSdkType;
import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowType.fromSdkType;

import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;

public class MetricHelper {

    public static void recordMetrics(final PollForDecisionTaskResponse decisionTask, final Metrics metrics) {
        metrics.addProperty(MetricName.Property.TASK_STARTED_EVENT_ID.getName(), decisionTask.startedEventId().toString());
        metrics.addProperty(MetricName.Property.TASK_TOKEN.getName(), decisionTask.taskToken());
        recordMetrics(fromSdkType(decisionTask.workflowExecution()), metrics);
        recordMetrics(fromSdkType(decisionTask.workflowType()), metrics);
    }

    public static void recordMetrics(final ActivityTask activityTask, final Metrics metrics) {
        metrics.addProperty(MetricName.Property.TASK_STARTED_EVENT_ID.getName(), activityTask.getStartedEventId().toString());
        metrics.addProperty(MetricName.Property.TASK_TOKEN.getName(), activityTask.getTaskToken());
        recordMetrics(activityTask.getWorkflowExecution(), metrics);
        recordMetrics(activityTask.getActivityType(), metrics);
    }

    public static void recordMetrics(final WorkflowExecution workflowExecution, final Metrics metrics) {
        metrics.addProperty(MetricName.Property.WORKFLOW_ID.getName(), workflowExecution.getWorkflowId());
        metrics.addProperty(MetricName.Property.RUN_ID.getName(), workflowExecution.getRunId());
    }

    public static void recordMetrics(final WorkflowType workflowType, final Metrics metrics) {
        metrics.addProperty(MetricName.Property.WORKFLOW_TYPE.getName(), workflowType.getName() + "@" + workflowType.getVersion());
        metrics.addProperty(MetricName.Property.WORKFLOW_TYPE_NAME.getName(), workflowType.getName());
        metrics.addProperty(MetricName.Property.WORKFLOW_TYPE_VERSION.getName(), workflowType.getVersion());
    }

    public static void recordMetrics(final ActivityType activityType, final Metrics metrics) {
        metrics.addProperty(MetricName.Property.ACTIVITY_TYPE.getName(), activityType.getName() + "@" + activityType.getVersion());
        metrics.addProperty(MetricName.Property.ACTIVITY_TYPE_NAME.getName(), activityType.getName());
        metrics.addProperty(MetricName.Property.ACTIVITY_TYPE_VERSION.getName(), activityType.getVersion());
    }
}
