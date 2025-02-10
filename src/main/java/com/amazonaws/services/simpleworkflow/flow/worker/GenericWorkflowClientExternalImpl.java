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

import static com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers.secondsToDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericWorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.flow.generic.SignalExternalWorkflowParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.StartWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.TerminateWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.DescribeWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.DescribeWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.RequestCancelWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.SignalWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.TerminateWorkflowExecutionRequest;

public class GenericWorkflowClientExternalImpl implements GenericWorkflowClientExternal {

    private final String domain;

    private final SwfClient service;

    private SimpleWorkflowClientConfig config;

    public GenericWorkflowClientExternalImpl(SwfClient service, String domain) {
        this(service, domain, null);
    }

    public GenericWorkflowClientExternalImpl(SwfClient service, String domain, SimpleWorkflowClientConfig config) {
        this.service = service;
        this.domain = domain;
        this.config = config;
    }

    @Override
    public WorkflowExecution startWorkflow(StartWorkflowExecutionParameters startParameters) {
        StartWorkflowExecutionRequest.Builder builderRequest = StartWorkflowExecutionRequest.builder()
            .domain(domain)
            .input(startParameters.getInput())
            .executionStartToCloseTimeout(secondsToDuration(startParameters.getExecutionStartToCloseTimeout()))
            .taskStartToCloseTimeout(secondsToDuration(startParameters.getTaskStartToCloseTimeoutSeconds()))
            .tagList(startParameters.getTagList()).workflowId(startParameters.getWorkflowId())
            .workflowType(startParameters.getWorkflowType().toSdkType())
            .taskPriority(FlowHelpers.taskPriorityToString(startParameters.getTaskPriority()))
            .lambdaRole(startParameters.getLambdaRole());

        String taskList = startParameters.getTaskList();
        if (taskList != null && !taskList.isEmpty()) {
            builderRequest.taskList(TaskList.builder().name(taskList).build());
        }

        if (startParameters.getChildPolicy() != null) {
            builderRequest.childPolicy(startParameters.getChildPolicy());
        }

        StartWorkflowExecutionRequest request = builderRequest.build();

        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        StartWorkflowExecutionResponse result = service.startWorkflowExecution(request);
        return WorkflowExecution.builder().workflowId(request.workflowId()).runId(result.runId()).build();
    }

    @Override
    public void signalWorkflowExecution(SignalExternalWorkflowParameters signalParameters) {
        SignalWorkflowExecutionRequest request = SignalWorkflowExecutionRequest.builder()
            .domain(domain)
            .input(signalParameters.getInput())
            .signalName(signalParameters.getSignalName())
            .runId(signalParameters.getRunId())
            .workflowId(signalParameters.getWorkflowId())
            .build();

        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.signalWorkflowExecution(request);
    }

    @Override
    public void requestCancelWorkflowExecution(WorkflowExecution execution) {
        RequestCancelWorkflowExecutionRequest request = RequestCancelWorkflowExecutionRequest.builder().domain(domain)
            .runId(execution.getRunId()).workflowId(execution.getWorkflowId()).build();

        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.requestCancelWorkflowExecution(request);
    }

    @Override
    public String generateUniqueId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getWorkflowState(WorkflowExecution execution) {
        String executionContext = getLatestWorkflowExecutionContext(execution);
        String result;
        // Should be in sync with HistoryHelper.updateWorkflowContextDataAndComponentVersions
        if (executionContext != null && executionContext.startsWith(AsyncDecisionTaskHandler.COMPONENT_VERSION_MARKER)) {
            Scanner scanner = new Scanner(executionContext);
            scanner.useDelimiter(AsyncDecisionTaskHandler.COMPONENT_VERSION_SEPARATORS_PATTERN);
            scanner.next();
            int size = scanner.nextInt();
            for (int i = 0; i < size; i++) {
                // component name
                scanner.next();
                // version
                scanner.nextInt();
            }
            result = scanner.next(".*");
        }
        else {
            result = executionContext;
        }
        return result;
    }
 
    @Override
    public Map<String, Integer> getImplementationVersions(WorkflowExecution execution) {
        String executionContext = getLatestWorkflowExecutionContext(execution);
        Map<String, Integer> result = new HashMap<String, Integer>();
        // Should be in sync with HistoryHelper.updateWorkflowContextDataAndComponentVersions
        if (executionContext.startsWith(AsyncDecisionTaskHandler.COMPONENT_VERSION_MARKER)) {
            Scanner scanner = new Scanner(executionContext);
            scanner.useDelimiter(AsyncDecisionTaskHandler.COMPONENT_VERSION_SEPARATORS_PATTERN);
            scanner.next();
            int size = scanner.nextInt();
            for (int i = 0; i < size; i++) {
                String componentName = scanner.next();
                int version = scanner.nextInt();
                result.put(componentName, version);
            }
        }
        return result;
    }

    private String getLatestWorkflowExecutionContext(WorkflowExecution execution) {
        DescribeWorkflowExecutionRequest request = DescribeWorkflowExecutionRequest.builder().domain(domain).execution(execution.toSdkType()).build();

        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        DescribeWorkflowExecutionResponse details = service.describeWorkflowExecution(request);
        return details.latestExecutionContext();
    }

    @Override
    public void terminateWorkflowExecution(TerminateWorkflowExecutionParameters terminateParameters) {
        WorkflowExecution workflowExecution = terminateParameters.getWorkflowExecution();
        TerminateWorkflowExecutionRequest request = TerminateWorkflowExecutionRequest.builder()
            .workflowId(workflowExecution.getWorkflowId())
            .runId(workflowExecution.getRunId())
            .domain(domain)
            .details(terminateParameters.getDetails())
            .reason(terminateParameters.getReason())
            .childPolicy(terminateParameters.getChildPolicy()).build();

        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.terminateWorkflowExecution(request);
    }
}
