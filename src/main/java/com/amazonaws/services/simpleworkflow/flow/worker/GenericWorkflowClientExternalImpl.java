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

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericWorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.flow.generic.SignalExternalWorkflowParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.StartWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.TerminateWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.model.DescribeWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.RequestCancelWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.Run;
import com.amazonaws.services.simpleworkflow.model.SignalWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.StartWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;
import com.amazonaws.services.simpleworkflow.model.TerminateWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionDetail;

public class GenericWorkflowClientExternalImpl implements GenericWorkflowClientExternal {

    private final String domain;

    private final AmazonSimpleWorkflow service;

    private SimpleWorkflowClientConfig config;

    public GenericWorkflowClientExternalImpl(AmazonSimpleWorkflow service, String domain) {
        this(service, domain, null);
    }

    public GenericWorkflowClientExternalImpl(AmazonSimpleWorkflow service, String domain, SimpleWorkflowClientConfig config) {
        this.service = service;
        this.domain = domain;
        this.config = config;
    }

    @Override
    public WorkflowExecution startWorkflow(StartWorkflowExecutionParameters startParameters) {
        StartWorkflowExecutionRequest request = new StartWorkflowExecutionRequest();
        request.setDomain(domain);

        request.setInput(startParameters.getInput());
        request.setExecutionStartToCloseTimeout(FlowHelpers.secondsToDuration(startParameters.getExecutionStartToCloseTimeout()));
        request.setTaskStartToCloseTimeout(FlowHelpers.secondsToDuration(startParameters.getTaskStartToCloseTimeoutSeconds()));
        request.setTagList(startParameters.getTagList());
        String taskList = startParameters.getTaskList();
        if (taskList != null && !taskList.isEmpty()) {
            request.setTaskList(new TaskList().withName(taskList));
        }
        request.setWorkflowId(startParameters.getWorkflowId());
        request.setWorkflowType(startParameters.getWorkflowType());
        request.setTaskPriority(FlowHelpers.taskPriorityToString(startParameters.getTaskPriority()));

        if(startParameters.getChildPolicy() != null) {
            request.setChildPolicy(startParameters.getChildPolicy());
        }
        request.setLambdaRole(startParameters.getLambdaRole());

        RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        Run result = service.startWorkflowExecution(request);
        WorkflowExecution execution = new WorkflowExecution().withRunId(result.getRunId()).withWorkflowId(request.getWorkflowId());

        return execution;
    }

    @Override
    public void signalWorkflowExecution(SignalExternalWorkflowParameters signalParameters) {
        SignalWorkflowExecutionRequest request = new SignalWorkflowExecutionRequest();
        request.setDomain(domain);

        request.setInput(signalParameters.getInput());
        request.setSignalName(signalParameters.getSignalName());
        request.setRunId(signalParameters.getRunId());
        request.setWorkflowId(signalParameters.getWorkflowId());

        RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.signalWorkflowExecution(request);
    }

    @Override
    public void requestCancelWorkflowExecution(WorkflowExecution execution) {
        RequestCancelWorkflowExecutionRequest request = new RequestCancelWorkflowExecutionRequest();
        request.setDomain(domain);

        request.setRunId(execution.getRunId());
        request.setWorkflowId(execution.getWorkflowId());

        RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.requestCancelWorkflowExecution(request);
    }

    @Override
    public String generateUniqueId() {
        String workflowId = UUID.randomUUID().toString();
        return workflowId;
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
        DescribeWorkflowExecutionRequest request = new DescribeWorkflowExecutionRequest();
        request.setDomain(domain);
        request.setExecution(execution);

        RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        WorkflowExecutionDetail details = service.describeWorkflowExecution(request);
        String executionContext = details.getLatestExecutionContext();
        return executionContext;
    }

    @Override
    public void terminateWorkflowExecution(TerminateWorkflowExecutionParameters terminateParameters) {
        TerminateWorkflowExecutionRequest request = new TerminateWorkflowExecutionRequest();
        WorkflowExecution workflowExecution = terminateParameters.getWorkflowExecution();
        request.setWorkflowId(workflowExecution.getWorkflowId());
        request.setRunId(workflowExecution.getRunId());
        request.setDomain(domain);
        request.setDetails(terminateParameters.getDetails());
        request.setReason(terminateParameters.getReason());
        request.setChildPolicy(terminateParameters.getChildPolicy());

        RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.terminateWorkflowExecution(request);
    }
}
