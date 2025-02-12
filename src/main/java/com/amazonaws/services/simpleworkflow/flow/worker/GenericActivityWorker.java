/**
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow.worker;

import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementation;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityTask;
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
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.RegisterActivityTypeRequest;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.TypeAlreadyExistsException;

public class GenericActivityWorker extends GenericWorker<ActivityTask> {

    private static final Log log = LogFactory.getLog(GenericActivityWorker.class);

    public static final String POLL_THREAD_NAME_PREFIX = "SWF Activity ";

    @Getter
    @Setter
    private ActivityImplementationFactory activityImplementationFactory;

    public GenericActivityWorker(SwfClient service, String domain, String taskListToPoll) {
        this(service, domain, taskListToPoll, null);
    }

    public GenericActivityWorker(SwfClient service, String domain, String taskListToPoll, SimpleWorkflowClientConfig config) {
        super(service, domain, taskListToPoll, config);
        if (service == null) {
            throw new IllegalArgumentException("service");
        }
    }

    public GenericActivityWorker() {
        super();
    }

    GenericActivityWorker(SwfClient service, String domain, String taskListToPoll,
        ScheduledExecutorService pollerExecutor, ThreadPoolExecutor workerExecutor, ActivityTaskPoller activityTaskPoller, Boolean startRequested) {
        super(service, domain, taskListToPoll, pollerExecutor, workerExecutor, activityTaskPoller, startRequested);
    }

    @Override
    protected TaskPoller<ActivityTask> createPoller() {
        ActivityTaskPoller activityTaskPoller = new ActivityTaskPoller(service, domain, getTaskListToPoll(),
                activityImplementationFactory, getExecuteThreadCount(), getClientConfig());

        activityTaskPoller.setIdentity(getIdentity());
        activityTaskPoller.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        activityTaskPoller.setMetricsRegistry(getMetricsRegistry());
        return activityTaskPoller;
    }

    @Override
    public void registerTypesToPoll() {
        registerActivityTypes(service, domain, getTaskListToPoll(), activityImplementationFactory, getClientConfig());
    }

    public static void registerActivityTypes(SwfClient service, String domain, String defaultTaskList,
            ActivityImplementationFactory activityImplementationFactory) {
        registerActivityTypes(service, domain, defaultTaskList, activityImplementationFactory, null);
    }

    public static void registerActivityTypes(SwfClient service, String domain, String defaultTaskList,
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

    public static void registerActivityType(SwfClient service, String domain, ActivityType activityType,
            ActivityTypeRegistrationOptions registrationOptions, String taskListToPoll) throws AwsServiceException {
        registerActivityType(service, domain, activityType, registrationOptions, taskListToPoll, null);
    }

    public static void registerActivityType(SwfClient service, String domain, ActivityType activityType,
                                            ActivityTypeRegistrationOptions registrationOptions, String taskListToPoll,
                                            SimpleWorkflowClientConfig config) throws AwsServiceException {
        RegisterActivityTypeRequest registerActivity = buildRegisterActivityTypeRequest(domain, activityType, registrationOptions, taskListToPoll);

        registerActivity = RequestTimeoutHelper.overrideControlPlaneRequestTimeout(registerActivity, config);
        registerActivityTypeWithRetry(service, registerActivity);
        if (log.isInfoEnabled()) {
            log.info("registered activity type: " + activityType);
        }
    }

    private static RegisterActivityTypeRequest buildRegisterActivityTypeRequest(String domain, ActivityType activityType,
            ActivityTypeRegistrationOptions registrationOptions, String taskListToPoll) throws AwsServiceException {
        RegisterActivityTypeRequest.Builder registerActivityBuilder = RegisterActivityTypeRequest.builder().domain(domain);
        String taskList = registrationOptions.getDefaultTaskList();
        if (taskList == null) {
            taskList = taskListToPoll;
        } else if (taskList.equals(FlowConstants.NO_DEFAULT_TASK_LIST)) {
            taskList = null;
        }
        if (taskList != null && !taskList.isEmpty()) {
            registerActivityBuilder.defaultTaskList(TaskList.builder().name(taskList).build());
        }
        registerActivityBuilder
            .name(activityType.getName())
            .version(activityType.getVersion())
            .defaultTaskStartToCloseTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskStartToCloseTimeoutSeconds()))
            .defaultTaskScheduleToCloseTimeout(
                FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskScheduleToCloseTimeoutSeconds()))
            .defaultTaskHeartbeatTimeout(FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskHeartbeatTimeoutSeconds()))
            .defaultTaskScheduleToStartTimeout(
                FlowHelpers.secondsToDuration(registrationOptions.getDefaultTaskScheduleToStartTimeoutSeconds()))
            .defaultTaskPriority(FlowHelpers.taskPriorityToString(registrationOptions.getDefaultTaskPriority()));

        if (registrationOptions.getDescription() != null) {
            registerActivityBuilder.description(registrationOptions.getDescription());
        }
        return registerActivityBuilder.build();
    }

    private static void registerActivityTypeWithRetry(SwfClient service, RegisterActivityTypeRequest registerActivityTypeRequest) {
        final ThrottlingRetrier retrier = new ThrottlingRetrier(getRegisterTypeThrottledRetryParameters());
        retrier.retry(
            () -> ThreadLocalMetrics.getMetrics().recordRunnable(() -> service.registerActivityType(registerActivityTypeRequest), MetricName.Operation.REGISTER_ACTIVITY_TYPE.getName(), TimeUnit.MILLISECONDS)
        );
    }

    @Override
    protected void checkRequiredProperties() {
        checkRequiredProperty(activityImplementationFactory, "activityImplementationFactory");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [super=" + super.toString() + ", taskExecutorThreadPoolSize="
                + getExecuteThreadCount() + "]";
    }

    @Override
    protected String getPollThreadNamePrefix() {
        return POLL_THREAD_NAME_PREFIX + getTaskListToPoll();
    }

    @Override
    protected WorkerType getWorkerType() {
        return WorkerType.ACTIVITY;
    }
}
