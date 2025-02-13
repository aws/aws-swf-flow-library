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

import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution.fromSdkType;
import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowType.fromSdkType;

import java.util.List;

import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import software.amazon.awssdk.services.swf.model.ChildPolicy;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionStartedEventAttributes;


class WorkflowContextImpl implements WorkflowContext {

    private final WorkflowClock clock;
    private final PollForDecisionTaskResponse decisionTask;
    private boolean cancelRequested;
    private ContinueAsNewWorkflowExecutionParameters continueAsNewOnCompletion;
    private ComponentVersions componentVersions;

    /**
     * DecisionTaskPoller.next has an optimization to remove the history page
     * from the first decision task. This is to keep a handle on the started
     * event attributes in the first event for future access.
     */
    private WorkflowExecutionStartedEventAttributes workflowStartedEventAttributes;

    public WorkflowContextImpl(PollForDecisionTaskResponse decisionTask, WorkflowClock clock) {
        this.decisionTask = decisionTask;
        this.clock = clock;
        HistoryEvent firstHistoryEvent = decisionTask.events().get(0);
        this.workflowStartedEventAttributes = firstHistoryEvent.workflowExecutionStartedEventAttributes();
    }
    
    @Override
    public WorkflowExecution getWorkflowExecution() {
        return fromSdkType(decisionTask.workflowExecution());
    }

    @Override
    public WorkflowType getWorkflowType() {
        return fromSdkType(decisionTask.workflowType());
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
        return fromSdkType(attributes.parentWorkflowExecution());
    }

    @Override
    public List<String> getTagList() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.tagList();
    }

    @Override
    public ChildPolicy getChildPolicy() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return ChildPolicy.fromValue(attributes.childPolicyAsString());
    }
    
    @Override
    public String getContinuedExecutionRunId() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.continuedExecutionRunId();
    }
    
    @Override
    public long getExecutionStartToCloseTimeout() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        String result = attributes.executionStartToCloseTimeout();
        return FlowHelpers.durationToSeconds(result);
    }
    
    @Override
    public String getTaskList() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.taskList().name();
    }

    @Override
    public String getLambdaRole() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        return attributes.lambdaRole();
    }

    private WorkflowExecutionStartedEventAttributes getWorkflowStartedEventAttributes() {
        return workflowStartedEventAttributes;
    }

    @Override
    public int getTaskPriority() {
        WorkflowExecutionStartedEventAttributes attributes = getWorkflowStartedEventAttributes();
        String result = attributes.taskPriority();
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
