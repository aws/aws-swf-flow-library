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
import software.amazon.awssdk.services.swf.model.StartChildWorkflowExecutionFailedCause;

@SuppressWarnings("serial")
public class StartChildWorkflowFailedException extends ChildWorkflowException {

    private StartChildWorkflowExecutionFailedCause failureCause;
    
    public StartChildWorkflowFailedException(String message) {
        super(message);
    }

    public StartChildWorkflowFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public StartChildWorkflowFailedException(long eventId, WorkflowExecution workflowExecution, WorkflowType workflowType,
            String cause) {
        super(cause, eventId, workflowExecution, workflowType);
        this.failureCause = StartChildWorkflowExecutionFailedCause.fromValue(cause);
    }

    /**
     * @return enumeration that contains the cause of the failure
     */
    public StartChildWorkflowExecutionFailedCause getFailureCause() {
        return failureCause;
    }

    public void setFailureCause(StartChildWorkflowExecutionFailedCause failureCause) {
        this.failureCause = failureCause;
    }

}
