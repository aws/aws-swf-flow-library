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

@SuppressWarnings("serial")
public class ChildWorkflowFailedException extends ChildWorkflowException {

    private String details;
    
    public ChildWorkflowFailedException(String message) {
        super(message);
    }
  
    public ChildWorkflowFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ChildWorkflowFailedException(long eventId, WorkflowExecution workflowExecution, WorkflowType workflowType,
            String reason, String details) {
        super(createMessage(workflowExecution, workflowType, reason), eventId, workflowExecution, workflowType);
        this.details = details;
    }

    public String getDetails() {
        return details;
    }
    
    public void setDetails(String details) {
        this.details = details;
    }
    
    private static String createMessage(WorkflowExecution workflowExecution, WorkflowType workflowType, String reason) {
        return "name=" + workflowType.getName() + ", version=" + workflowType.getVersion() + ", workflowId="
                + workflowExecution.getWorkflowId() + ", runId=" + workflowExecution.getRunId() + ": " + reason;
    }
}
