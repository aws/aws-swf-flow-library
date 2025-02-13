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
public class ChildWorkflowTimedOutException extends ChildWorkflowException {
    
    public ChildWorkflowTimedOutException(String message) {
        super(message);
    }

    public ChildWorkflowTimedOutException(String message, Throwable cause) {
        super(message, cause);
    }

    public ChildWorkflowTimedOutException(long eventId, WorkflowExecution workflowExecution, WorkflowType workflowType) {
        super("Time Out", eventId, workflowExecution, workflowType);
    }

}
