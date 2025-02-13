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
package com.amazonaws.services.simpleworkflow.flow.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder(toBuilder = true)
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityTask {

    private String taskToken;

    private String activityId;

    private Long startedEventId;

    private WorkflowExecution workflowExecution;

    private ActivityType activityType;

    private String input;


    public static ActivityTask fromSdkType(
        software.amazon.awssdk.services.swf.model.PollForActivityTaskResponse sdkResponse) {
        if (sdkResponse == null) {
            return null;
        }

        return ActivityTask.builder()
            .activityId(sdkResponse.activityId())
            .activityType(ActivityType.fromSdkType(sdkResponse.activityType()))
            .input(sdkResponse.input())
            .workflowExecution(WorkflowExecution.fromSdkType(sdkResponse.workflowExecution()))
            .startedEventId(sdkResponse.startedEventId())
            .taskToken(sdkResponse.taskToken())
            .build();
    }

    public software.amazon.awssdk.services.swf.model.PollForActivityTaskResponse toSdkType() {
        return software.amazon.awssdk.services.swf.model.PollForActivityTaskResponse.builder()
            .activityId(this.activityId)
            .activityType(this.activityType.toSdkType())
            .input(this.input)
            .workflowExecution(this.workflowExecution.toSdkType())
            .activityType(this.activityType.toSdkType())
            .taskToken(this.taskToken)
            .startedEventId(startedEventId)
            .build();
    }

}
