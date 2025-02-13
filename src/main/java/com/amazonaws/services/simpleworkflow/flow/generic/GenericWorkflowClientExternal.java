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

import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import java.util.Map;


public interface GenericWorkflowClientExternal {
    
    public WorkflowExecution startWorkflow(StartWorkflowExecutionParameters startParameters);
    
    public void signalWorkflowExecution(SignalExternalWorkflowParameters signalParameters);
    
    public void requestCancelWorkflowExecution(WorkflowExecution execution);
    
    public String getWorkflowState(WorkflowExecution execution);
    
    /**
     * Gets the implementation versions for workflow component
     *
     * @param execution the workflow execution to get implementation versions for
     * @return a map containing component names as keys and their implementation versions as values
     *
     * @see WorkflowContext#isImplementationVersion(String, int)
     */
    public Map<String, Integer> getImplementationVersions(WorkflowExecution execution);
    
    public void terminateWorkflowExecution(TerminateWorkflowExecutionParameters terminateParameters);

    public String generateUniqueId();

}
