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
package com.amazonaws.services.simpleworkflow.flow.test;

import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.core.TryCatchFinally;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import java.util.List;
import software.amazon.awssdk.services.swf.model.ChildPolicy;


public class TestWorkflowContext implements WorkflowContext {

    private WorkflowExecution workflowExecution;
    private WorkflowType workflowType;
    private ContinueAsNewWorkflowExecutionParameters continueAsNewOnCompletion;
    private WorkflowExecution parentWorkflowExecution;
    private List<String> tagList;
    private ChildPolicy childPolicy;
    private String continuedExecutionRunId;
    private long executionStartToCloseTimeout;
    private String taskList;
    private int taskPriority;
    private String lambdaRole;
    private TryCatchFinally rootTryCatch;
    
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
    
    public ContinueAsNewWorkflowExecutionParameters getContinueAsNewOnCompletion() {
        return continueAsNewOnCompletion;
    }
    
    public void setContinueAsNewOnCompletion(ContinueAsNewWorkflowExecutionParameters continueAsNewOnCompletion) {
        this.continueAsNewOnCompletion = continueAsNewOnCompletion;
    }
    
    public WorkflowExecution getParentWorkflowExecution() {
        return parentWorkflowExecution;
    }
    
    public void setParentWorkflowExecution(WorkflowExecution parentWorkflowExecution) {
        this.parentWorkflowExecution = parentWorkflowExecution;
    }
    
    public List<String> getTagList() {
        return tagList;
    }
    
    public void setTagList(List<String> tagList) {
        this.tagList = tagList;
    }
    
    public ChildPolicy getChildPolicy() {
        return childPolicy;
    }
    
    public void setChildPolicy(ChildPolicy childPolicy) {
        this.childPolicy = childPolicy;
    }
    
    public String getContinuedExecutionRunId() {
        return continuedExecutionRunId;
    }
    
    public void setContinuedExecutionRunId(String continuedExecutionRunId) {
        this.continuedExecutionRunId = continuedExecutionRunId;
    }
    
    public long getExecutionStartToCloseTimeout() {
        return executionStartToCloseTimeout;
    }
    
    public void setExecutionStartToCloseTimeout(long executionStartToCloseTimeout) {
        this.executionStartToCloseTimeout = executionStartToCloseTimeout;
    }
    
    public String getTaskList() {
        return taskList;
    }
    
    public void setTaskList(String taskList) {
        this.taskList = taskList;
    }

    public String getLambdaRole() {
        return lambdaRole;
    }

    public void setLambdaRole(String lambdaRole) {
        this.lambdaRole = lambdaRole;
    }
    
    public boolean isCancelRequested() {
        return rootTryCatch.isCancelRequested();
    }
    
    public void setRootTryCatch(TryCatchFinally rootTryCatch) {
        this.rootTryCatch = rootTryCatch;
    }

    public int getTaskPriority() {
        return taskPriority;
    }

    public void setTaskPriority(int taskPriority) {
        this.taskPriority = taskPriority;
    }
	
	@Override
    public boolean isImplementationVersion(String component, int version) {
        return true;
    }
 
    @Override
    public Integer getVersion(String component) {
        return null;
    }
}
