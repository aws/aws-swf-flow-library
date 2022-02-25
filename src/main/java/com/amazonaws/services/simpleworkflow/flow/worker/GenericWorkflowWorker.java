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

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.ChildWorkflowIdHandler;
import com.amazonaws.services.simpleworkflow.flow.DefaultChildWorkflowIdHandler;
import com.amazonaws.services.simpleworkflow.flow.WorkflowTypeRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.retry.ThrottlingRetrier;
import com.amazonaws.services.simpleworkflow.model.RegisterWorkflowTypeRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;
import com.amazonaws.services.simpleworkflow.model.TypeAlreadyExistsException;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GenericWorkflowWorker extends GenericWorker<DecisionTaskPoller.DecisionTaskIterator> {

    private static final Log log = LogFactory.getLog(GenericWorkflowWorker.class);

    private static final String THREAD_NAME_PREFIX = "SWF Decider ";

    @Getter
    @Setter
    private WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory;

    @Setter
    private ChildWorkflowIdHandler childWorkflowIdHandler = new DefaultChildWorkflowIdHandler();

    public GenericWorkflowWorker() {
        super();
    }

    public GenericWorkflowWorker(AmazonSimpleWorkflow service, String domain, String taskListToPoll) {
        super(service, domain, taskListToPoll, null);
    }

    public GenericWorkflowWorker(AmazonSimpleWorkflow service, String domain, String taskListToPoll, SimpleWorkflowClientConfig config) {
        super(service, domain, taskListToPoll, config);
    }

    protected DecisionTaskPoller createWorkflowPoller() {
        return new DecisionTaskPoller();
    }

    @Override
    protected TaskPoller<DecisionTaskPoller.DecisionTaskIterator> createPoller() {
        DecisionTaskPoller result = new DecisionTaskPoller();
        result.setDecisionTaskHandler(new AsyncDecisionTaskHandler(workflowDefinitionFactoryFactory, childWorkflowIdHandler));
        result.setDomain(getDomain());
        result.setIdentity(getIdentity());
        result.setService(getService());
        result.setTaskListToPoll(getTaskListToPoll());
        return result;
    }

    @Override
    public void registerTypesToPoll() {
        registerWorkflowTypes(service, domain, getTaskListToPoll(), workflowDefinitionFactoryFactory);
    }

    public static void registerWorkflowTypes(AmazonSimpleWorkflow service, String domain, String defaultTaskList,
                                             WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory) {
        registerWorkflowTypes(service, domain, defaultTaskList, workflowDefinitionFactoryFactory, null);
    }

    public static void registerWorkflowTypes(AmazonSimpleWorkflow service, String domain, String defaultTaskList,
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

    public static void registerWorkflowType(AmazonSimpleWorkflow service, String domain, WorkflowType workflowType,
                                            WorkflowTypeRegistrationOptions registrationOptions, String defaultTaskList) {
        registerWorkflowType(service, domain, workflowType, registrationOptions, defaultTaskList, null);
    }

    public static void registerWorkflowType(AmazonSimpleWorkflow service, String domain, WorkflowType workflowType,
                                            WorkflowTypeRegistrationOptions registrationOptions, String defaultTaskList, SimpleWorkflowClientConfig config) {
        RegisterWorkflowTypeRequest registerWorkflow = buildRegisterWorkflowTypeRequest(domain, workflowType, registrationOptions, defaultTaskList);

        RequestTimeoutHelper.overrideControlPlaneRequestTimeout(registerWorkflow, config);
        registerWorkflowTypeWithRetry(service, registerWorkflow);
    }

    private static RegisterWorkflowTypeRequest buildRegisterWorkflowTypeRequest(String domain, WorkflowType workflowType,
                                                                                WorkflowTypeRegistrationOptions registrationOptions, String defaultTaskList) {
        RegisterWorkflowTypeRequest registerWorkflow = new RegisterWorkflowTypeRequest();

        registerWorkflow.setDomain(domain);
        registerWorkflow.setName(workflowType.getName());
        registerWorkflow.setVersion(workflowType.getVersion());
        String taskList = registrationOptions.getDefaultTaskList();
        if (taskList == null) {
            taskList = defaultTaskList;
        }
        else if (taskList.equals(FlowConstants.NO_DEFAULT_TASK_LIST)) {
            taskList = null;
        }
        if (taskList != null && !taskList.isEmpty()) {
            registerWorkflow.setDefaultTaskList(new TaskList().withName(taskList));
        }
        registerWorkflow.setDefaultChildPolicy(registrationOptions.getDefaultChildPolicy().toString());
        registerWorkflow.setDefaultTaskStartToCloseTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskStartToCloseTimeoutSeconds()));
        registerWorkflow.setDefaultExecutionStartToCloseTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultExecutionStartToCloseTimeoutSeconds()));
        registerWorkflow.setDefaultTaskPriority(FlowHelpers.taskPriorityToString(registrationOptions.getDefaultTaskPriority()));
        registerWorkflow.setDefaultLambdaRole(registrationOptions.getDefaultLambdaRole());

        String description = registrationOptions.getDescription();
        if (description != null) {
            registerWorkflow.setDescription(description);
        }

        return registerWorkflow;
    }

    private static void registerWorkflowTypeWithRetry(AmazonSimpleWorkflow service,
                                                      RegisterWorkflowTypeRequest registerWorkflowTypeRequest) {
        ThrottlingRetrier retrier = new ThrottlingRetrier(getRegisterTypeThrottledRetryParameters());
        retrier.retry(() -> service.registerWorkflowType(registerWorkflowTypeRequest));
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
        return THREAD_NAME_PREFIX + getTaskListToPoll() + " ";
    }
}
