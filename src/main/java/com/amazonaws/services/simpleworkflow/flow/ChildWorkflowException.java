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
package com.amazonaws.services.simpleworkflow.flow;

import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;

/**
 * Exception used to communicate failure of remote activity.
 */
@SuppressWarnings("serial")
public abstract class ChildWorkflowException extends DecisionException {
    
    private WorkflowExecution workflowExecution;
    
    private WorkflowType workflowType;
    
    public ChildWorkflowException(String message) {
        super(message);
    }

    public ChildWorkflowException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public ChildWorkflowException(String message, long eventId, WorkflowExecution workflowExecution, WorkflowType workflowType) {
        super(message + " for workflowExecution=\"" + workflowExecution + "\" of workflowType=" + workflowType, eventId);
        this.workflowExecution = workflowExecution;
        this.workflowType = workflowType;
    }
    
    public WorkflowExecution getWorkflowExecution() {
        return workflowExecution;
    }
    
    public void setWorkflowExecution(WorkflowExecution workflowExecution) {
        this.workflowExecution = workflowExecution;
    }
    
    public WorkflowType getWorkflowType() {
        return workflowType;
    }
    
    public void setWorkflowType(WorkflowType workflowType) {
        this.workflowType = workflowType;
    }
}
