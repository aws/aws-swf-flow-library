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

import com.amazonaws.services.simpleworkflow.flow.StartWorkflowOptions;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import java.util.List;
import java.util.Objects;
import software.amazon.awssdk.services.swf.model.ChildPolicy;

public class StartChildWorkflowExecutionParameters implements Cloneable {

    private String control;

    private long executionStartToCloseTimeoutSeconds = FlowConstants.USE_REGISTERED_DEFAULTS;

    private String input;

    private List<String> tagList;

    private String taskList;

    private long taskStartToCloseTimeoutSeconds = FlowConstants.USE_REGISTERED_DEFAULTS;

    private String workflowId;

    private WorkflowType workflowType;

    private ChildPolicy childPolicy;

    private int taskPriority;

    private String lambdaRole;

    public StartChildWorkflowExecutionParameters() {
    }

    public String getControl() {
        return control;
    }

    public void setControl(String control) {
        this.control = control;
    }

    public StartChildWorkflowExecutionParameters withControl(String control) {
        this.control = control;
        return this;
    }

    public long getExecutionStartToCloseTimeoutSeconds() {
        return executionStartToCloseTimeoutSeconds;
    }

    public void setExecutionStartToCloseTimeoutSeconds(long executionStartToCloseTimeoutSeconds) {
        this.executionStartToCloseTimeoutSeconds = executionStartToCloseTimeoutSeconds;
    }

    public StartChildWorkflowExecutionParameters withExecutionStartToCloseTimeoutSeconds(long executionStartToCloseTimeoutSeconds) {
        this.executionStartToCloseTimeoutSeconds = executionStartToCloseTimeoutSeconds;
        return this;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public StartChildWorkflowExecutionParameters withInput(String input) {
        this.input = input;
        return this;
    }

    public List<String> getTagList() {
        return tagList;
    }

    public void setTagList(List<String> tagList) {
        this.tagList = tagList;
    }

    public StartChildWorkflowExecutionParameters withTagList(List<String> tagList) {
        this.tagList = tagList;
        return this;
    }

    public String getTaskList() {
        return taskList;
    }

    public void setTaskList(String taskList) {
        this.taskList = taskList;
    }

    public StartChildWorkflowExecutionParameters withTaskList(String taskList) {
        this.taskList = taskList;
        return this;
    }

    public long getTaskStartToCloseTimeoutSeconds() {
        return taskStartToCloseTimeoutSeconds;
    }

    public void setTaskStartToCloseTimeoutSeconds(long taskStartToCloseTimeoutSeconds) {
        this.taskStartToCloseTimeoutSeconds = taskStartToCloseTimeoutSeconds;
    }

    public StartChildWorkflowExecutionParameters withTaskStartToCloseTimeoutSeconds(long taskStartToCloseTimeoutSeconds) {
        this.taskStartToCloseTimeoutSeconds = taskStartToCloseTimeoutSeconds;
        return this;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public StartChildWorkflowExecutionParameters withWorkflowId(String workflowId) {
        this.workflowId = workflowId;
        return this;
    }

    public WorkflowType getWorkflowType() {
        return workflowType;
    }

    public void setWorkflowType(WorkflowType workflowType) {
        this.workflowType = workflowType;
    }

    public StartChildWorkflowExecutionParameters withWorkflowType(WorkflowType workflowType) {
        this.workflowType = workflowType;
        return this;
    }

    public ChildPolicy getChildPolicy() {
        return childPolicy;
    }

    public void setChildPolicy(ChildPolicy childPolicy) {
        this.childPolicy = childPolicy;
    }

    public StartChildWorkflowExecutionParameters withChildPolicy(ChildPolicy childPolicy) {
        this.childPolicy = childPolicy;
        return this;
    }

    public int getTaskPriority() {
        return taskPriority;
    }

    public void setTaskPriority(int taskPriority) {
        this.taskPriority = taskPriority;
    }

    public StartChildWorkflowExecutionParameters withTaskPriority(int taskPriority) {
        this.taskPriority = taskPriority;
        return this;
    }

    public String getLambdaRole() {
        return lambdaRole;
    }

    public void setLambdaRole(String lambdaRole) {
        this.lambdaRole = lambdaRole;
    }

    public StartChildWorkflowExecutionParameters withLambdaRole(String lambdaRole) {
        this.lambdaRole = lambdaRole;
        return this;
    }

    public StartChildWorkflowExecutionParameters createStartChildWorkflowExecutionParametersFromOptions(
            StartWorkflowOptions options, StartWorkflowOptions optionsOverride) {
        StartChildWorkflowExecutionParameters startChildWorkflowExecutionParameters = this.clone();

        if (options != null) {
            setParametersFromStartWorkflowOptions(startChildWorkflowExecutionParameters, options);
        }

        if (optionsOverride != null) {
            setParametersFromStartWorkflowOptions(startChildWorkflowExecutionParameters, optionsOverride);
        }

        return startChildWorkflowExecutionParameters;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("WorkflowType: " + workflowType + ", ");
        sb.append("WorkflowId: " + workflowId + ", ");
        sb.append("Input: " + input + ", ");
        sb.append("Control: " + control + ", ");
        sb.append("ExecutionStartToCloseTimeout: " + executionStartToCloseTimeoutSeconds + ", ");
        sb.append("TaskStartToCloseTimeout: " + taskStartToCloseTimeoutSeconds + ", ");
        sb.append("TagList: " + tagList + ", ");
        sb.append("TaskList: " + taskList + ", ");
        sb.append("TaskPriority: " + taskPriority + ", ");
        sb.append("LambdaRole: " + lambdaRole);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public StartChildWorkflowExecutionParameters clone() {
        StartChildWorkflowExecutionParameters result = new StartChildWorkflowExecutionParameters();
        result.setControl(control);
        result.setExecutionStartToCloseTimeoutSeconds(executionStartToCloseTimeoutSeconds);
        result.setInput(input);
        result.setTagList(tagList);
        result.setTaskList(taskList);
        result.setTaskStartToCloseTimeoutSeconds(taskStartToCloseTimeoutSeconds);
        result.setWorkflowId(workflowId);
        result.setWorkflowType(workflowType);
        result.setTaskPriority(taskPriority);
        result.setLambdaRole(lambdaRole);
        return result;
    }

    private void setParametersFromStartWorkflowOptions(final StartChildWorkflowExecutionParameters destinationParameters, final StartWorkflowOptions options) {
        Objects.requireNonNull(destinationParameters, "destinationParameters");
        Objects.requireNonNull(options, "options");

        Long executionStartToCloseTimeoutSeconds = options.getExecutionStartToCloseTimeoutSeconds();
        if (executionStartToCloseTimeoutSeconds != null) {
            destinationParameters.setExecutionStartToCloseTimeoutSeconds(executionStartToCloseTimeoutSeconds);
        }

        Long taskStartToCloseTimeoutSeconds = options.getTaskStartToCloseTimeoutSeconds();
        if (taskStartToCloseTimeoutSeconds != null) {
            destinationParameters.setTaskStartToCloseTimeoutSeconds(taskStartToCloseTimeoutSeconds);
        }

        Integer taskPriority = options.getTaskPriority();
        if (taskPriority != null) {
            destinationParameters.setTaskPriority(taskPriority);
        }

        List<String> tagList = options.getTagList();
        if (tagList != null) {
            destinationParameters.setTagList(tagList);
        }

        String taskList = options.getTaskList();
        if (taskList != null && !taskList.isEmpty()) {
            destinationParameters.setTaskList(taskList);
        }

        ChildPolicy childPolicy = options.getChildPolicy();
        if (childPolicy != null) {
            destinationParameters.setChildPolicy(childPolicy);
        }

        String lambdaRole = options.getLambdaRole();
        if (lambdaRole != null) {
            destinationParameters.setLambdaRole(lambdaRole);
        }
    }
}
