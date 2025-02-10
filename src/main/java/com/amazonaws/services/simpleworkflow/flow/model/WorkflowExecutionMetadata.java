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
package com.amazonaws.services.simpleworkflow.flow.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder(toBuilder = true)
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowExecutionMetadata {

    private String taskList;

    private WorkflowType workflowType;

    private List<String> tagList;

    public static WorkflowExecutionMetadata fromSdkType(
        software.amazon.awssdk.services.swf.model.WorkflowExecutionStartedEventAttributes attributes) {
        return attributes == null ? null
            : WorkflowExecutionMetadata.builder()
                .taskList(attributes.taskList().name())
                .workflowType(WorkflowType.fromSdkType(attributes.workflowType()))
                .tagList(attributes.tagList())
                .build();
    }

}
