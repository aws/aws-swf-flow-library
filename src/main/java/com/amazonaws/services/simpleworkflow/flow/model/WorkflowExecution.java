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
public class WorkflowExecution {

    private String workflowId;

    private String runId;

    public static WorkflowExecution fromSdkType(
        software.amazon.awssdk.services.swf.model.WorkflowExecution workflowExecution) {
        return workflowExecution == null ? null
            : WorkflowExecution.builder().workflowId(workflowExecution.workflowId()).runId(workflowExecution.runId()).build();
    }

    public software.amazon.awssdk.services.swf.model.WorkflowExecution toSdkType() {
        return software.amazon.awssdk.services.swf.model.WorkflowExecution.builder().workflowId(this.workflowId)
                .runId(this.runId).build();
    }
}
