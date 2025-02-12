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

import com.amazonaws.services.simpleworkflow.flow.ChildWorkflowIdHandler;
import com.amazonaws.services.simpleworkflow.flow.DefaultChildWorkflowIdHandler;
import com.amazonaws.services.simpleworkflow.flow.WorkflowTypeRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import com.amazonaws.services.simpleworkflow.flow.retry.ThrottlingRetrier;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.RegisterWorkflowTypeRequest;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.TypeAlreadyExistsException;

public class GenericWorkflowWorker extends GenericWorker<DecisionTaskPoller.DecisionTaskIterator> {

    private static final Log log = LogFactory.getLog(GenericWorkflowWorker.class);

    public static final String THREAD_NAME_PREFIX = "SWF Decider ";

    @Getter
    @Setter
    protected WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory;

    @Setter
    protected ChildWorkflowIdHandler childWorkflowIdHandler = new DefaultChildWorkflowIdHandler();

    public GenericWorkflowWorker() {
        super();
    }

    public GenericWorkflowWorker(SwfClient service, String domain, String taskListToPoll) {
        super(service, domain, taskListToPoll, null);
    }

    public GenericWorkflowWorker(SwfClient service, String domain, String taskListToPoll, SimpleWorkflowClientConfig config) {
        super(service, domain, taskListToPoll, config);
    }

    GenericWorkflowWorker(SwfClient service, String domain, String taskListToPoll,
        ScheduledExecutorService pollerExecutor, ThreadPoolExecutor workerExecutor, DecisionTaskPoller decisionTaskPoller, Boolean startRequested) {
        super(service, domain, taskListToPoll, pollerExecutor, workerExecutor, decisionTaskPoller, startRequested);
    }

    @Override
    protected TaskPoller<DecisionTaskPoller.DecisionTaskIterator> createPoller() {
        DecisionTaskPoller result = new DecisionTaskPoller();
        SimpleWorkflowClientConfig config = getClientConfig() == null ? SimpleWorkflowClientConfig.ofDefaults() : getClientConfig();
        AffinityHelper affinityHelper = new AffinityHelper(getService(), getDomain(), getClientConfig(), false);
        result.setDecisionTaskHandler(new AsyncDecisionTaskHandler(workflowDefinitionFactoryFactory, childWorkflowIdHandler, affinityHelper, config));
        result.setDomain(getDomain());
        result.setIdentity(getIdentity());
        result.setService(getService());
        result.setTaskListToPoll(getTaskListToPoll());
        result.setMetricsRegistry(getMetricsRegistry());
        result.setConfig(getClientConfig());
        return result;
    }

    @Override
    public void registerTypesToPoll() {
        registerWorkflowTypes(service, domain, getTaskListToPoll(), workflowDefinitionFactoryFactory);
    }

    public static void registerWorkflowTypes(SwfClient service, String domain, String defaultTaskList,
            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory) {
        registerWorkflowTypes(service, domain, defaultTaskList, workflowDefinitionFactoryFactory, null);
    }

    public static void registerWorkflowTypes(SwfClient service, String domain, String defaultTaskList,
             WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory, SimpleWorkflowClientConfig config) {
        for (WorkflowType typeToRegister : workflowDefinitionFactoryFactory.getWorkflowTypesToRegister()) {
            WorkflowDefinitionFactory workflowDefinitionFactory = workflowDefinitionFactoryFactory.getWorkflowDefinitionFactory(typeToRegister);
            WorkflowTypeRegistrationOptions registrationOptions = workflowDefinitionFactory.getWorkflowRegistrationOptions();
            if (registrationOptions != null) {
                WorkflowType workflowType = workflowDefinitionFactory.getWorkflowType();
                try {
                    registerWorkflowType(service, domain, workflowType, registrationOptions, defaultTaskList, config);
                }
                catch (TypeAlreadyExistsException ex) {
                    if (log.isTraceEnabled()) {
                        log.trace("Workflow Type already registered: " + workflowType);
                    }
                }
            }
        }
    }

    public static void registerWorkflowType(SwfClient service, String domain, WorkflowType workflowType,
            WorkflowTypeRegistrationOptions registrationOptions, String defaultTaskList) {
        registerWorkflowType(service, domain, workflowType, registrationOptions, defaultTaskList, null);
    }

    public static void registerWorkflowType(SwfClient service, String domain, WorkflowType workflowType,
            WorkflowTypeRegistrationOptions registrationOptions, String defaultTaskList, SimpleWorkflowClientConfig config) {
        RegisterWorkflowTypeRequest registerWorkflow = buildRegisterWorkflowTypeRequest(domain, workflowType, registrationOptions, defaultTaskList);

        registerWorkflow = RequestTimeoutHelper.overrideControlPlaneRequestTimeout(registerWorkflow, config);
        registerWorkflowTypeWithRetry(service, registerWorkflow);
    }

    private static RegisterWorkflowTypeRequest buildRegisterWorkflowTypeRequest(String domain, WorkflowType workflowType,
            WorkflowTypeRegistrationOptions registrationOptions, String defaultTaskList) {
        RegisterWorkflowTypeRequest.Builder registerWorkflowBuilder
            = RegisterWorkflowTypeRequest.builder().domain(domain).name(workflowType.getName()).version(workflowType.getVersion());
        String taskList = registrationOptions.getDefaultTaskList();
        if (taskList == null) {
            taskList = defaultTaskList;
        }
        else if (taskList.equals(FlowConstants.NO_DEFAULT_TASK_LIST)) {
            taskList = null;
        }
        if (taskList != null && !taskList.isEmpty()) {
            registerWorkflowBuilder.defaultTaskList(TaskList.builder().name(taskList).build());
        }

        registerWorkflowBuilder.defaultChildPolicy(registrationOptions.getDefaultChildPolicy().toString())
            .defaultTaskStartToCloseTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskStartToCloseTimeoutSeconds()))
            .defaultExecutionStartToCloseTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultExecutionStartToCloseTimeoutSeconds()))
            .defaultTaskPriority(FlowHelpers.taskPriorityToString(registrationOptions.getDefaultTaskPriority()))
            .defaultLambdaRole(registrationOptions.getDefaultLambdaRole());

        String description = registrationOptions.getDescription();
        if (description != null) {
            registerWorkflowBuilder.description(description);
        }

        return registerWorkflowBuilder.build();
    }

    private static void registerWorkflowTypeWithRetry(SwfClient service, RegisterWorkflowTypeRequest registerWorkflowTypeRequest) {
        final ThrottlingRetrier retrier = new ThrottlingRetrier(getRegisterTypeThrottledRetryParameters());
        retrier.retry(
            () -> ThreadLocalMetrics.getMetrics().recordRunnable(() -> service.registerWorkflowType(registerWorkflowTypeRequest), MetricName.Operation.REGISTER_WORKFLOW_TYPE.getName(), TimeUnit.MILLISECONDS)
        );
    }

    @Override
    protected void checkRequiredProperties() {
        checkRequiredProperty(workflowDefinitionFactoryFactory, "workflowDefinitionFactoryFactory");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[super=" + super.toString() + ", workflowDefinitionFactoryFactory="
                + workflowDefinitionFactoryFactory + "]";
    }

    @Override
    protected String getPollThreadNamePrefix() {
        return THREAD_NAME_PREFIX + getTaskListToPoll();
    }

    @Override
    protected WorkerType getWorkerType() {
        return WorkerType.WORKFLOW;
    }
}
