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
package com.amazonaws.services.simpleworkflow.flow.worker;

import java.util.List;

import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.model.ChildPolicy;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.HistoryEvent;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionStartedEventAttributes;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;


class WorkflowContextImpl implements WorkflowContext {

    private final WorkflowClock clock;
    private final DecisionTask decisionTask;
    private boolean cancelRequested;
    private ContinueAsNewWorkflowExecutionParameters continueAsNewOnCompletion;
    private ComponentVersions componentVersions;

    /**
     * DecisionTaskPoller.next has an optimization to remove the history page
     * from the first decision task. This is to keep a handle on the started
     * event attributes in the first event for future access.
     */
    private WorkflowExecutionStartedEventAttributes workflowStartedEventAttributes;

    public WorkflowContextImpl(DecisionTask decisionTask, WorkflowClock clock) {
        this.decisionTask = decisionTask;
        this.clock = clock;
        HistoryEvent firstHistoryEvent = decisionTask.getEvents().get(0);
        this.workflowStartedEventAttributes = firstHistoryEvent.getWorkflowExecutionStartedEventAttributes();
    }
    
    @Override
    public WorkflowExecution getWorkflowExecution() {
        return decisionTask.getWorkflowExecution();
    }

    @Override
    public WorkflowType getWorkflowType() {
        return decisionTask.getWorkflowType();
    }

    @Override
    public boolean isCancelRequested() {
        return cancelRequested;
    }

    void setCancelRequested(boolean flag) {
        cancelRequested = flag;
    }

    @Override
    public ContinueAsNewWorkflowExecutionParameters getContinueAsNewOnCompletion() {
        return continueAsNewOnCompletion;
    }

    @Override
    public void setContinueAsNewOnCompletion(ContinueAsNewWorkflowExecutionParameters continueParameters) {
        this.continueAsNewOnCompletion = continueParameters;
    }

    @Override
    public WorkflowExecution getParentWorkflowExecution() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.getParentWorkflowExecution();
    }

    @Override
    public List<String> getTagList() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.getTagList();
    }

    @Override
    public ChildPolicy getChildPolicy() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return ChildPolicy.fromValue(attributes.getChildPolicy());
    }
    
    @Override
    public String getContinuedExecutionRunId() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.getContinuedExecutionRunId();
    }
    
    @Override
    public long getExecutionStartToCloseTimeout() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        String result = attributes.getExecutionStartToCloseTimeout();
        return FlowHelpers.durationToSeconds(result);
    }
    
    @Override
    public String getTaskList() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.getTaskList().getName();
    }

    @Override
    public String getLambdaRole() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.getLambdaRole();
    }

    private WorkflowExecutionStartedEventAttributes getWorkflowStartedEventAttributes() {
        return workflowStartedEventAttributes;
    }

    @Override
    public int getTaskPriority() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        String result = attributes.getTaskPriority();
        return FlowHelpers.taskPriorityToInt(result);
    }
    
    @Override
    public boolean isImplementationVersion(String component, int version) {
       return componentVersions.isVersion(component, version, clock.isReplaying());
    }
 
    @Override
    public Integer getVersion(String component) {
        return componentVersions.getCurrentVersion(component);
    }
 
    void setComponentVersions(ComponentVersions componentVersions) {
        this.componentVersions = componentVersions;
    }
    
}
