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
package com.amazonaws.services.simpleworkflow.flow.generic;

import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import software.amazon.awssdk.services.swf.model.ChildPolicy;

public class TerminateWorkflowExecutionParameters {

    private WorkflowExecution workflowExecution;

    private ChildPolicy childPolicy;

    private String reason;

    private String details;

    public TerminateWorkflowExecutionParameters() {
    }

    public TerminateWorkflowExecutionParameters(WorkflowExecution workflowExecution, ChildPolicy childPolicy, String reason,
            String details) {
        this.workflowExecution = workflowExecution;
        this.childPolicy = childPolicy;
        this.reason = reason;
        this.details = details;
    }

    public WorkflowExecution getWorkflowExecution() {
        return workflowExecution;
    }

    public void setWorkflowExecution(WorkflowExecution workflowExecution) {
        this.workflowExecution = workflowExecution;
    }

    public TerminateWorkflowExecutionParameters withWorkflowExecution(WorkflowExecution workflowExecution) {
        this.workflowExecution = workflowExecution;
        return this;
    }

    public ChildPolicy getChildPolicy() {
        return childPolicy;
    }

    public void setChildPolicy(ChildPolicy childPolicy) {
        this.childPolicy = childPolicy;
    }

    public TerminateWorkflowExecutionParameters withChildPolicy(ChildPolicy childPolicy) {
        this.childPolicy = childPolicy;
        return this;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public TerminateWorkflowExecutionParameters withReason(String reason) {
        this.reason = reason;
        return this;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public TerminateWorkflowExecutionParameters withDetails(String details) {
        this.details = details;
        return this;
    }

}
