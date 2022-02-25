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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementation;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.retry.ThrottlingRetrier;
import com.amazonaws.services.simpleworkflow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.model.RegisterActivityTypeRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;
import com.amazonaws.services.simpleworkflow.model.TypeAlreadyExistsException;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GenericActivityWorker extends GenericWorker<ActivityTask> {

    private static final Log log = LogFactory.getLog(GenericActivityWorker.class);

    private static final String POLL_THREAD_NAME_PREFIX = "SWF Activity ";

    @Getter
    @Setter
    private ActivityImplementationFactory activityImplementationFactory;

    public GenericActivityWorker(AmazonSimpleWorkflow service, String domain, String taskListToPoll) {
        this(service, domain, taskListToPoll, null);
    }

    public GenericActivityWorker(AmazonSimpleWorkflow service, String domain, String taskListToPoll, SimpleWorkflowClientConfig config) {
        super(service, domain, taskListToPoll, config);
        if (service == null) {
            throw new IllegalArgumentException("service");
        }
    }

    public GenericActivityWorker() {
        super();
    }

    @Override
    protected TaskPoller<ActivityTask> createPoller() {
        ActivityTaskPoller activityTaskPoller = new ActivityTaskPoller(service, domain, getTaskListToPoll(),
                activityImplementationFactory, executeThreadCount, getClientConfig());

        activityTaskPoller.setIdentity(getIdentity());
        activityTaskPoller.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return activityTaskPoller;
    }

    @Override
    public void registerTypesToPoll() {
        registerActivityTypes(service, domain, getTaskListToPoll(), activityImplementationFactory, getClientConfig());
    }

    public static void registerActivityTypes(AmazonSimpleWorkflow service, String domain, String defaultTaskList,
                                             ActivityImplementationFactory activityImplementationFactory) {
        registerActivityTypes(service, domain, defaultTaskList, activityImplementationFactory, null);
    }

    public static void registerActivityTypes(AmazonSimpleWorkflow service, String domain, String defaultTaskList,
                                             ActivityImplementationFactory activityImplementationFactory, SimpleWorkflowClientConfig config) {
        for (ActivityType activityType : activityImplementationFactory.getActivityTypesToRegister()) {
            try {
                ActivityImplementation implementation = activityImplementationFactory.getActivityImplementation(activityType);
                if (implementation == null) {
                    throw new IllegalStateException("No implementation found for type needed registration: " + activityType);
                }
                ActivityTypeRegistrationOptions registrationOptions = implementation.getRegistrationOptions();
                if (registrationOptions != null) {
                    registerActivityType(service, domain, activityType, registrationOptions, defaultTaskList, config);
                }
            }
            catch (TypeAlreadyExistsException ex) {
                if (log.isTraceEnabled()) {
                    log.trace("Activity version already registered: " + activityType.getName() + "_" + activityType.getVersion());
                }
            }
        }
    }

    public static void registerActivityType(AmazonSimpleWorkflow service, String domain, ActivityType activityType,
                                            ActivityTypeRegistrationOptions registrationOptions, String taskListToPoll) throws AmazonServiceException {
        registerActivityType(service, domain, activityType, registrationOptions, taskListToPoll, null);
    }

    public static void registerActivityType(AmazonSimpleWorkflow service, String domain, ActivityType activityType,
                                            ActivityTypeRegistrationOptions registrationOptions, String taskListToPoll,
                                            SimpleWorkflowClientConfig config) throws AmazonServiceException {
        RegisterActivityTypeRequest registerActivity = buildRegisterActivityTypeRequest(domain, activityType, registrationOptions, taskListToPoll);

        RequestTimeoutHelper.overrideControlPlaneRequestTimeout(registerActivity, config);
        registerActivityTypeWithRetry(service, registerActivity);
        if (log.isInfoEnabled()) {
            log.info("registered activity type: " + activityType);
        }
    }

    private static RegisterActivityTypeRequest buildRegisterActivityTypeRequest(String domain, ActivityType activityType,
                                                                                ActivityTypeRegistrationOptions registrationOptions, String taskListToPoll) throws AmazonServiceException {
        RegisterActivityTypeRequest registerActivity = new RegisterActivityTypeRequest();
        registerActivity.setDomain(domain);
        String taskList = registrationOptions.getDefaultTaskList();
        if (taskList == null) {
            taskList = taskListToPoll;
        }
        else if (taskList.equals(FlowConstants.NO_DEFAULT_TASK_LIST)) {
            taskList = null;
        }
        if (taskList != null && !taskList.isEmpty()) {
            registerActivity.setDefaultTaskList(new TaskList().withName(taskList));
        }
        registerActivity.setName(activityType.getName());
        registerActivity.setVersion(activityType.getVersion());
        registerActivity.setDefaultTaskStartToCloseTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskStartToCloseTimeoutSeconds()));
        registerActivity.setDefaultTaskScheduleToCloseTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskScheduleToCloseTimeoutSeconds()));
        registerActivity.setDefaultTaskHeartbeatTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskHeartbeatTimeoutSeconds()));
        registerActivity.setDefaultTaskScheduleToStartTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskScheduleToStartTimeoutSeconds()));
        registerActivity.setDefaultTaskPriority(FlowHelpers.taskPriorityToString(registrationOptions.getDefaultTaskPriority()));

        if (registrationOptions.getDescription() != null) {
            registerActivity.setDescription(registrationOptions.getDescription());
        }
        return registerActivity;
    }

    private static void registerActivityTypeWithRetry(AmazonSimpleWorkflow service,
                                                      RegisterActivityTypeRequest registerActivityTypeRequest) {
        ThrottlingRetrier retrier = new ThrottlingRetrier(getRegisterTypeThrottledRetryParameters());
        retrier.retry(() -> service.registerActivityType(registerActivityTypeRequest));
    }

    @Override
    protected void checkRequiredProperties() {
        checkRequiredProperty(activityImplementationFactory, "activityImplementationFactory");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [super=" + super.toString() + ", taskExecutorThreadPoolSize="
                + executeThreadCount + "]";
    }

    @Override
    protected String getPollThreadNamePrefix() {
        return POLL_THREAD_NAME_PREFIX + getTaskListToPoll() + " ";
    }
}
