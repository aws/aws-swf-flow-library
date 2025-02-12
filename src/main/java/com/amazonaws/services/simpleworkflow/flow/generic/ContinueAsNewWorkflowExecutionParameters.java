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
import java.util.List;
import software.amazon.awssdk.services.swf.model.ChildPolicy;


public class ContinueAsNewWorkflowExecutionParameters {

    private String workflowTypeVersion;
    private long executionStartToCloseTimeoutSeconds = FlowConstants.USE_REGISTERED_DEFAULTS;
    private String input;
    private List<String> tagList;
    private String taskList;
    private long taskStartToCloseTimeoutSeconds = FlowConstants.USE_REGISTERED_DEFAULTS;
    private ChildPolicy childPolicy;
    private int taskPriority;
    private String lambdaRole;

    public ContinueAsNewWorkflowExecutionParameters() {
    }

    public String getWorkflowTypeVersion() {
        return workflowTypeVersion;
    }

    public void setWorkflowTypeVersion(String workflowTypeVersion) {
        this.workflowTypeVersion = workflowTypeVersion;
    }

    public ChildPolicy getChildPolicy() {
        return childPolicy;
    }

    public void setChildPolicy(ChildPolicy childPolicy) {
        this.childPolicy = childPolicy;
    }

    public long getExecutionStartToCloseTimeoutSeconds() {
        return executionStartToCloseTimeoutSeconds;
    }

    public void setExecutionStartToCloseTimeoutSeconds(long executionStartToCloseTimeoutSeconds) {
        this.executionStartToCloseTimeoutSeconds = executionStartToCloseTimeoutSeconds;
    }

    public ContinueAsNewWorkflowExecutionParameters withExecutionStartToCloseTimeoutSeconds(long executionStartToCloseTimeoutSeconds) {
        this.executionStartToCloseTimeoutSeconds = executionStartToCloseTimeoutSeconds;
        return this;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public ContinueAsNewWorkflowExecutionParameters withInput(String input) {
        this.input = input;
        return this;
    }

    public List<String> getTagList() {
        return tagList;
    }

    public void setTagList(List<String> tagList) {
        this.tagList = tagList;
    }

    public ContinueAsNewWorkflowExecutionParameters withTagList(List<String> tagList) {
        this.tagList = tagList;
        return this;
    }

    public String getTaskList() {
        return taskList;
    }

    public void setTaskList(String taskList) {
        this.taskList = taskList;
    }

    public ContinueAsNewWorkflowExecutionParameters withTaskList(String taskList) {
        this.taskList = taskList;
        return this;
    }

    public int getTaskPriority() {
        return taskPriority;
    }

    public void setTaskPriority(int taskPriority) {
        this.taskPriority = taskPriority;
    }

    public ContinueAsNewWorkflowExecutionParameters withTaskPriority(int taskPriority) {
        this.taskPriority = taskPriority;
        return this;
    }

    public long getTaskStartToCloseTimeoutSeconds() {
        return taskStartToCloseTimeoutSeconds;
    }

    public void setTaskStartToCloseTimeoutSeconds(long taskStartToCloseTimeoutSeconds) {
        this.taskStartToCloseTimeoutSeconds = taskStartToCloseTimeoutSeconds;
    }

    public ContinueAsNewWorkflowExecutionParameters withTaskStartToCloseTimeoutSeconds(long taskStartToCloseTimeoutSeconds) {
        this.taskStartToCloseTimeoutSeconds = taskStartToCloseTimeoutSeconds;
        return this;
    }

    public String getLambdaRole() {
        return lambdaRole;
    }

    public void setLambdaRole(String lambdaRole) {
        this.lambdaRole = lambdaRole;
    }

    public ContinueAsNewWorkflowExecutionParameters withLambdaRole(String lambdaRole) {
        this.lambdaRole = lambdaRole;
        return this;
    }

    public ContinueAsNewWorkflowExecutionParameters createContinueAsNewParametersFromOptions(StartWorkflowOptions options,
        StartWorkflowOptions optionsOverride) {
        ContinueAsNewWorkflowExecutionParameters continueAsNewWorkflowExecutionParameters = this.clone();

        updateContinueAsNewWorkflowExecutionParameters(options, continueAsNewWorkflowExecutionParameters);
        updateContinueAsNewWorkflowExecutionParameters(optionsOverride, continueAsNewWorkflowExecutionParameters);

        return continueAsNewWorkflowExecutionParameters;
    }

    private void updateContinueAsNewWorkflowExecutionParameters(StartWorkflowOptions options,
        ContinueAsNewWorkflowExecutionParameters continueAsNewWorkflowExecutionParameters) {
        if (options != null) {
            Long executionStartToCloseTimeoutSeconds = options.getExecutionStartToCloseTimeoutSeconds();
            if (executionStartToCloseTimeoutSeconds != null) {
                continueAsNewWorkflowExecutionParameters.setExecutionStartToCloseTimeoutSeconds(executionStartToCloseTimeoutSeconds);
            }

            Long taskStartToCloseTimeoutSeconds = options.getTaskStartToCloseTimeoutSeconds();
            if (taskStartToCloseTimeoutSeconds != null) {
                continueAsNewWorkflowExecutionParameters.setTaskStartToCloseTimeoutSeconds(taskStartToCloseTimeoutSeconds);
            }

            List<String> tagList = options.getTagList();
            if (tagList != null) {
                continueAsNewWorkflowExecutionParameters.setTagList(tagList);
            }

            String taskList = options.getTaskList();
            if (taskList != null && !taskList.isEmpty()) {
                continueAsNewWorkflowExecutionParameters.setTaskList(taskList);
            }

            ChildPolicy childPolicy = options.getChildPolicy();
            if (childPolicy != null) {
                continueAsNewWorkflowExecutionParameters.setChildPolicy(childPolicy);
            }

            Integer taskPriority = options.getTaskPriority();
            if (taskPriority != null) {
                continueAsNewWorkflowExecutionParameters.setTaskPriority(taskPriority);
            }

            String lambdaRole = options.getLambdaRole();
            if (lambdaRole != null) {
                continueAsNewWorkflowExecutionParameters.setLambdaRole(lambdaRole);
            }

            String workflowTypeVersion = options.getWorkflowTypeVersion();
            if (workflowTypeVersion != null && !workflowTypeVersion.trim().isEmpty()) {
                continueAsNewWorkflowExecutionParameters.setWorkflowTypeVersion(workflowTypeVersion);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("Input: " + input + ", ");
        sb.append("ExecutionStartToCloseTimeout: " + executionStartToCloseTimeoutSeconds + ", ");
        sb.append("TaskStartToCloseTimeout: " + taskStartToCloseTimeoutSeconds + ", ");
        sb.append("TagList: " + tagList + ", ");
        sb.append("TaskList: " + taskList + ", ");
        sb.append("TaskPriority: " + taskPriority + ", ");
        sb.append("LambdaRole: " + lambdaRole);
        sb.append("}");
        return sb.toString();
    }

    public ContinueAsNewWorkflowExecutionParameters clone() {
        ContinueAsNewWorkflowExecutionParameters result = new ContinueAsNewWorkflowExecutionParameters();
        result.setWorkflowTypeVersion(workflowTypeVersion);
        result.setExecutionStartToCloseTimeoutSeconds(executionStartToCloseTimeoutSeconds);
        result.setInput(input);
        result.setTagList(tagList);
        result.setTaskList(taskList);
        result.setTaskStartToCloseTimeoutSeconds(taskStartToCloseTimeoutSeconds);
        result.setChildPolicy(childPolicy);
        result.setTaskPriority(taskPriority);
        result.setLambdaRole(lambdaRole);
        return result;
    }

}
