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

import com.amazonaws.services.simpleworkflow.flow.ActivityExecutionContext;
import com.amazonaws.services.simpleworkflow.flow.ActivityFailureException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowValueConstraint;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementation;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricHelper;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.Metrics;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.NullMetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import com.amazonaws.services.simpleworkflow.flow.retry.SynchronousRetrier;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.PollForActivityTaskRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCanceledRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCompletedRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskFailedRequest;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.UnknownResourceException;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 */
public class ActivityTaskPoller implements TaskPoller<ActivityTask> {

    private static final Log log = LogFactory.getLog(ActivityTaskPoller.class);

    private static final long SECOND = 1000;

    @Getter
    @Setter
    private SwfClient service;

    @Getter
    @Setter
    private String domain;

    @Getter
    private String taskListToPoll;

    @Getter
    @Setter
    private ActivityImplementationFactory activityImplementationFactory;

    @Getter
    @Setter
    private String identity;

    private boolean initialized;

    private boolean suspended;

    private final Lock lock = new ReentrantLock();

    private final Condition suspentionCondition = lock.newCondition();

    @Getter
    private SimpleWorkflowClientConfig config;

    @Getter
    @Setter
    private UncaughtExceptionHandler uncaughtExceptionHandler;

    @Getter
    @Setter
    private MetricsRegistry metricsRegistry;

    @Getter
    private SuspendableSemaphore pollingSemaphore;

    ActivityTaskPoller() {
        identity = ManagementFactory.getRuntimeMXBean().getName();
        int length = Math.min(identity.length(), GenericWorker.MAX_IDENTITY_LENGTH);
        identity = identity.substring(0, length);
        metricsRegistry = new NullMetricsRegistry();
    }

    public ActivityTaskPoller(SwfClient service, String domain, String pollTaskList,
            ActivityImplementationFactory activityImplementationFactory) {
        this(service, domain, pollTaskList, activityImplementationFactory, null);
    }

    public ActivityTaskPoller(SwfClient service, String domain, String pollTaskList,
                              ActivityImplementationFactory activityImplementationFactory, SimpleWorkflowClientConfig config) {
        this(service, domain, pollTaskList, activityImplementationFactory, GenericWorker.DEFAULT_EXECUTOR_THREAD_COUNT, config);
    }

    public ActivityTaskPoller(SwfClient service, String domain, String pollTaskList,
                              ActivityImplementationFactory activityImplementationFactory, int executeThreadCount, SimpleWorkflowClientConfig config) {
        this();
        this.service = service;
        this.domain = domain;
        this.taskListToPoll = pollTaskList;
        this.activityImplementationFactory = activityImplementationFactory;
        this.config = config;
        pollingSemaphore = new SuspendableSemaphore(executeThreadCount);
    }

    private Exception wrapFailure(final ActivityTask task, Throwable failure) {
        WorkflowExecution execution = task.getWorkflowExecution();

        RuntimeException e2 = new RuntimeException(
                "Failure taskId=\"" + task.getStartedEventId() + "\" workflowExecutionRunId=\"" + execution.getRunId()
                        + "\" workflowExecutionId=\"" + execution.getWorkflowId() + "\"", failure);
        return e2;
    }

    @Override
    public ActivityTask poll() throws InterruptedException {
        waitIfSuspended();
        if (!initialized) {
            checkRequiredProperty(service, "service");
            checkRequiredProperty(domain, "domain");
            checkRequiredProperty(taskListToPoll, "taskListToPoll");
            initialized = true;
        }

        final Metrics metrics = metricsRegistry.newMetrics(MetricName.Operation.ACTIVITY_TASK_POLL.getName());
        metrics.addProperty(MetricName.Property.TASK_LIST.getName(), taskListToPoll);
        metrics.addProperty(MetricName.Property.DOMAIN.getName(), domain);
        PollForActivityTaskRequest pollRequest = PollForActivityTaskRequest.builder()
            .domain(domain)
            .identity(identity)
            .taskList(TaskList.builder().name(taskListToPoll).build())
            .build();

        if (log.isDebugEnabled()) {
            log.debug("poll request begin: " + pollRequest);
        }

        pollRequest = RequestTimeoutHelper.overridePollRequestTimeout(pollRequest, config);
        ActivityTask result;
        try {
            PollForActivityTaskRequest finalPollRequest = pollRequest;
            result = ActivityTask.fromSdkType(metrics.recordSupplier(() -> service.pollForActivityTask(finalPollRequest),
                MetricName.Operation.POLL_FOR_ACTIVITY_TASK.getName(), TimeUnit.MILLISECONDS));
            if (result == null || result.getTaskToken() == null) {
                result = null;
            } else {
                MetricHelper.recordMetrics(result, metrics);
            }
            metrics.recordCount(MetricName.EMPTY_POLL_COUNT.getName(), result == null, MetricName.getOperationDimension(MetricName.Operation.POLL_FOR_ACTIVITY_TASK.getName()));
        } finally {
            metrics.close();
        }
        return result;
    }

    @Override
    public void execute(ActivityTask task) throws Exception {
        String output = null;
        ActivityType activityType = task.getActivityType();
        final Metrics metrics = metricsRegistry.newMetrics(MetricName.Operation.EXECUTE_ACTIVITY_TASK.getName());
        MetricHelper.recordMetrics(task, metrics);
        final Metrics childMetrics = metrics.newMetrics();
        boolean activityResultSubmitted = false;

        ActivityExecutionContext context = new ActivityExecutionContextImpl(service, domain, task, config, metricsRegistry);

        ActivityTypeExecutionOptions executionOptions = null;
        try {
            try {
                ActivityImplementation activityImplementation = activityImplementationFactory.getActivityImplementation(activityType);
                if (activityImplementation == null) {
                    metrics.recordCount(MetricName.TYPE_NOT_FOUND.getName(), 1, MetricName.getActivityTypeDimension(activityType));
                    Iterable<ActivityType> typesToRegister = activityImplementationFactory.getActivityTypesToRegister();
                    StringBuilder types = new StringBuilder();
                    types.append("[");
                    for (ActivityType t : typesToRegister) {
                        if (types.length() > 1) {
                            types.append(", ");
                        }
                        types.append(t);
                    }
                    types.append("]");
                    throw new ActivityFailureException("Activity type \"" + activityType
                        + "\" is not supported by the ActivityWorker. "
                        + "Possible cause is activity type version change without changing task list name. "
                        + "Activity types registered with the worker are: " + types);
                }
                ThreadLocalMetrics.setCurrent(childMetrics);
                executionOptions = activityImplementation.getExecutionOptions();
                output = childMetrics.recordCallable(() -> activityImplementation.execute(context), activityType.getName(),
                    TimeUnit.MILLISECONDS);

            } catch (CancellationException e) {
                respondActivityTaskCanceledWithRetry(task.getTaskToken(), null, executionOptions);
                activityResultSubmitted = true;
                return;

            } catch (ActivityFailureException e) {
                if (log.isErrorEnabled()) {
                    log.error("Failure processing activity task with taskId=" + task.getStartedEventId() + ", workflowGenerationId="
                        + task.getWorkflowExecution().getWorkflowId() + ", activity=" + activityType + ", activityInstanceId="
                        + task.getActivityId(), e);
                }
                respondActivityTaskFailedWithRetry(task.getTaskToken(), e.getReason(), e.getDetails(), executionOptions);
                activityResultSubmitted = true;
                return;

            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    log.error("Failure processing activity task with taskId=" + task.getStartedEventId() + ", workflowGenerationId="
                        + task.getWorkflowExecution().getWorkflowId() + ", activity=" + activityType + ", activityInstanceId="
                        + task.getActivityId(), e);
                }
                String reason = e.getMessage();
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String details = sw.toString();

                if (details.length() > FlowValueConstraint.FAILURE_DETAILS.getMaxSize()) {
                    metrics.recordCount(MetricName.RESPONSE_TRUNCATED.getName(), 1, MetricName.getActivityTypeDimension(activityType));
                    log.warn("Length of details is over maximum input length of 32768. Actual details: " + details +
                        "when processing activity task with taskId=" + task.getStartedEventId() + ", workflowId="
                        + task.getWorkflowExecution().getWorkflowId() + ", activity=" + activityType + ", activityInstanceId="
                        + task.getActivityId());
                    details = WorkflowExecutionUtils.truncateDetails(details);
                }
                respondActivityTaskFailedWithRetry(task.getTaskToken(), reason, details, executionOptions);
                activityResultSubmitted = true;
                return;
            } finally {
                childMetrics.close();
                ThreadLocalMetrics.setCurrent(metrics);
            }

            if (executionOptions == null || !executionOptions.isManualActivityCompletion()) {
                respondActivityTaskCompletedWithRetry(task.getTaskToken(), output, executionOptions);
                activityResultSubmitted = true;
            }
        } finally {
            metrics.recordCount(MetricName.DROPPED_TASK.getName(), !activityResultSubmitted, MetricName.getActivityTypeDimension(activityType));
            metrics.close();
            ThreadLocalMetrics.clearCurrent();
        }
    }

    private void waitIfSuspended() throws InterruptedException {
        lock.lock();
        try {
            while (suspended) {
                suspentionCondition.await();
            }
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void suspend() {
        lock.lock();
        try {
            suspended = true;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void resume() {
        lock.lock();
        try {
            suspended = false;
            suspentionCondition.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isSuspended() {
        lock.lock();
        try {
            return suspended;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
    }

    protected void checkRequiredProperty(Object value, String name) {
        if (value == null) {
            throw new IllegalStateException("required property " + name + " is not set");
        }
    }

    protected void respondActivityTaskFailed(String taskToken, String reason, String details) {
        RespondActivityTaskFailedRequest failedResponse = RespondActivityTaskFailedRequest.builder().taskToken(taskToken)
            .reason(WorkflowExecutionUtils.truncateReason(reason)).details(details).build();

        failedResponse = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(failedResponse, config);
        RespondActivityTaskFailedRequest finalFailedResponse = failedResponse;
        ThreadLocalMetrics.getMetrics().recordRunnable(() -> service.respondActivityTaskFailed(finalFailedResponse),
            MetricName.Operation.RESPOND_ACTIVITY_TASK_FAILED.getName(), TimeUnit.MILLISECONDS);
    }

    protected void respondActivityTaskCanceledWithRetry(final String taskToken, final String details,
                                                        ActivityTypeExecutionOptions executionOptions) {
        SynchronousRetrier retrier = null;
        if (executionOptions != null) {
            ActivityTypeCompletionRetryOptions completionRetryOptions = executionOptions.getCompletionRetryOptions();
            retrier = createRetrier(completionRetryOptions);
        }
        if (retrier != null) {
            retrier.retry(() -> respondActivityTaskCanceled(taskToken, details));
        }
        else {
            respondActivityTaskCanceled(taskToken, details);
        }
    }

    private SynchronousRetrier createRetrier(ActivityTypeCompletionRetryOptions activityTypeCompletionRetryOptions) {
        if (activityTypeCompletionRetryOptions == null) {
            return null;
        }
        ExponentialRetryParameters retryParameters = new ExponentialRetryParameters();
        retryParameters.setBackoffCoefficient(activityTypeCompletionRetryOptions.getBackoffCoefficient());
        retryParameters.setExpirationInterval(activityTypeCompletionRetryOptions.getRetryExpirationSeconds() * SECOND);
        retryParameters.setInitialInterval(activityTypeCompletionRetryOptions.getInitialRetryIntervalSeconds() * SECOND);
        retryParameters.setMaximumRetries(activityTypeCompletionRetryOptions.getMaximumAttempts() - 1);
        retryParameters.setMaximumRetryInterval(activityTypeCompletionRetryOptions.getMaximumRetryIntervalSeconds() * SECOND);
        retryParameters.setMinimumRetries(activityTypeCompletionRetryOptions.getMinimumAttempts() - 1);
        return new SynchronousRetrier(retryParameters, UnknownResourceException.class);
    }

    protected void respondActivityTaskCanceled(String taskToken, String details) {
        RespondActivityTaskCanceledRequest canceledResponse = RespondActivityTaskCanceledRequest.builder().taskToken(taskToken)
            .details(details).build();

        RespondActivityTaskCanceledRequest finalCanceledResponse = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(canceledResponse,
            config);
        ThreadLocalMetrics.getMetrics().recordRunnable(() -> service.respondActivityTaskCanceled(finalCanceledResponse),
            MetricName.Operation.RESPOND_ACTIVITY_TASK_CANCELED.getName(), TimeUnit.MILLISECONDS);
    }

    protected void respondActivityTaskCompletedWithRetry(final String taskToken, final String output,
                                                         ActivityTypeExecutionOptions executionOptions) {
        SynchronousRetrier retrier = null;
        if (executionOptions != null) {
            retrier = createRetrier(executionOptions.getCompletionRetryOptions());
        }
        if (retrier != null) {

            retrier.retry(() -> respondActivityTaskCompleted(taskToken, output));
        }
        else {
            respondActivityTaskCompleted(taskToken, output);
        }
    }

    protected void respondActivityTaskCompleted(String taskToken, String output) {
        RespondActivityTaskCompletedRequest completedResponse = RespondActivityTaskCompletedRequest.builder().taskToken(taskToken)
            .result(output).build();

        RespondActivityTaskCompletedRequest finalCompletedResponse = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(completedResponse,
            config);
        ThreadLocalMetrics.getMetrics().recordRunnable(() -> service.respondActivityTaskCompleted(finalCompletedResponse),
            MetricName.Operation.RESPOND_ACTIVITY_TASK_COMPLETED.getName(), TimeUnit.MILLISECONDS);
    }

    protected void respondActivityTaskFailedWithRetry(final String taskToken, final String reason, final String details,
                                                      ActivityTypeExecutionOptions executionOptions) {
        SynchronousRetrier retrier = null;
        if (executionOptions != null) {
            retrier = createRetrier(executionOptions.getFailureRetryOptions());
        }
        if (retrier != null) {
            retrier.retry(() -> respondActivityTaskFailed(taskToken, reason, details));
        }
        else {
            respondActivityTaskFailed(taskToken, reason, details);
        }
    }
}
