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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.ActivityExecutionContext;
import com.amazonaws.services.simpleworkflow.flow.ActivityFailureException;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementation;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementationFactory;
import com.amazonaws.services.simpleworkflow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.model.PollForActivityTaskRequest;
import com.amazonaws.services.simpleworkflow.model.RespondActivityTaskCanceledRequest;
import com.amazonaws.services.simpleworkflow.model.RespondActivityTaskCompletedRequest;
import com.amazonaws.services.simpleworkflow.model.RespondActivityTaskFailedRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;
import com.amazonaws.services.simpleworkflow.model.UnknownResourceException;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 *
 */
public class SynchronousActivityTaskPoller implements TaskPoller {

    private static final Log log = LogFactory.getLog(SynchronousActivityTaskPoller.class);

    private static final long SECOND = 1000;

    private static final int MAX_DETAIL_LENGTH = 32768;

    private AmazonSimpleWorkflow service;

    private String domain;

    private String taskListToPoll;

    private ActivityImplementationFactory activityImplementationFactory;

    private String identity;

    private boolean initialized;

    public SynchronousActivityTaskPoller(AmazonSimpleWorkflow service, String domain, String taskListToPoll,
            ActivityImplementationFactory activityImplementationFactory) {
        this();
        this.service = service;
        this.domain = domain;
        this.taskListToPoll = taskListToPoll;
        this.activityImplementationFactory = activityImplementationFactory;
    }

    public SynchronousActivityTaskPoller() {
        identity = ManagementFactory.getRuntimeMXBean().getName();
        int length = Math.min(identity.length(), GenericWorker.MAX_IDENTITY_LENGTH);
        identity = identity.substring(0, length);
    }

    public AmazonSimpleWorkflow getService() {
        return service;
    }

    public void setService(AmazonSimpleWorkflow service) {
        this.service = service;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getPollTaskList() {
        return taskListToPoll;
    }

    public void setTaskListToPoll(String taskList) {
        this.taskListToPoll = taskList;
    }

    public ActivityImplementationFactory getActivityImplementationFactory() {
        return activityImplementationFactory;
    }

    public void setActivityImplementationFactory(ActivityImplementationFactory activityImplementationFactory) {
        this.activityImplementationFactory = activityImplementationFactory;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public String getTaskListToPoll() {
        return taskListToPoll;
    }

    /**
     * Poll for a task using {@link #getPollTimeoutInSeconds()}
     * 
     * @return null if poll timed out
     */
    public ActivityTask poll() {
        if (!initialized) {
            checkRequiredProperty(service, "service");
            checkRequiredProperty(domain, "domain");
            checkRequiredProperty(taskListToPoll, "taskListToPoll");
            initialized = true;
        }

        PollForActivityTaskRequest pollRequest = new PollForActivityTaskRequest();
        pollRequest.setDomain(domain);
        pollRequest.setIdentity(identity);
        pollRequest.setTaskList(new TaskList().withName(taskListToPoll));
        if (log.isDebugEnabled()) {
            log.debug("poll request begin: " + pollRequest);
        }
        ActivityTask result = service.pollForActivityTask(pollRequest);
        if (result == null || result.getTaskToken() == null) {
            if (log.isDebugEnabled()) {
                log.debug("poll request returned no task");
            }
            return null;
        }
        if (log.isTraceEnabled()) {
            log.trace("poll request returned " + result);
        }
        return result;
    }

    /**
     * Poll for a activity task and execute correspondent implementation.
     * 
     * @return true if task was polled and decided upon, false if poll timed out
     * @throws Exception
     */
    @Override
    public boolean pollAndProcessSingleTask() throws Exception {
        ActivityTask task = poll();
        if (task == null) {
            return false;
        }
        execute(task);
        return true;
    }

    protected void execute(final ActivityTask task) throws Exception {
        String output = null;
        ActivityType activityType = task.getActivityType();
        ActivityExecutionContext context = new ActivityExecutionContextImpl(service, domain, task);
        ActivityTypeExecutionOptions executionOptions = null;
        try {
            ActivityImplementation activityImplementation = activityImplementationFactory.getActivityImplementation(activityType);
            if (activityImplementation == null) {
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
                        + "Activity types registered with the worker are: " + types.toString());
            }
            executionOptions = activityImplementation.getExecutionOptions();
            output = activityImplementation.execute(context);
        }
        catch (CancellationException e) {
            respondActivityTaskCanceledWithRetry(task.getTaskToken(), null, executionOptions);
            return;
        }
        catch (ActivityFailureException e) {
            if (log.isErrorEnabled()) {
                log.error("Failure processing activity task with taskId=" + task.getStartedEventId() + ", workflowGenerationId="
                        + task.getWorkflowExecution().getWorkflowId() + ", activity=" + activityType + ", activityInstanceId="
                        + task.getActivityId(), e);
            }
            respondActivityTaskFailedWithRetry(task.getTaskToken(), e.getReason(), e.getDetails(), executionOptions);
            return;
        }
        catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Failure processing activity task with taskId=" + task.getStartedEventId() + ", workflowGenerationId="
                        + task.getWorkflowExecution().getWorkflowId() + ", activity=" + activityType + ", activityInstanceId="
                        + task.getActivityId(), e);
            }
            String reason = e.getMessage();
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String details = sw.toString();
            if (details.length() > MAX_DETAIL_LENGTH) {
                log.warn("Length of details is over maximum input length of 32768. Actual details: " + details +
                        "when processing activity task with taskId=" + task.getStartedEventId() + ", workflowGenerationId="
                        + task.getWorkflowExecution().getWorkflowId() + ", activity=" + activityType + ", activityInstanceId="
                        + task.getActivityId());
                details = details.substring(0, MAX_DETAIL_LENGTH);
            }
            respondActivityTaskFailedWithRetry(task.getTaskToken(), reason, details, executionOptions);
            return;
        }
        if (executionOptions == null || !executionOptions.isManualActivityCompletion()) {
            respondActivityTaskCompletedWithRetry(task.getTaskToken(), output, executionOptions);
        }
    }

    protected void respondActivityTaskFailedWithRetry(final String taskToken, final String reason, final String details,
            ActivityTypeExecutionOptions executionOptions) {
        SynchronousRetrier retrier = null;
        if (executionOptions != null) {
            retrier = createRetrier(executionOptions.getFailureRetryOptions());
        }
        if (retrier != null) {

            retrier.retry(new Runnable() {

                @Override
                public void run() {
                    respondActivityTaskFailed(taskToken, reason, details);
                }
            });
        }
        else {
            respondActivityTaskFailed(taskToken, reason, details);
        }
    }

    protected void respondActivityTaskFailed(String taskToken, String reason, String details) {
        RespondActivityTaskFailedRequest failedResponse = new RespondActivityTaskFailedRequest();
        failedResponse.setTaskToken(taskToken);
        failedResponse.setReason(WorkflowExecutionUtils.truncateReason(reason));
        failedResponse.setDetails(details);
        service.respondActivityTaskFailed(failedResponse);
    }

    protected void respondActivityTaskCanceledWithRetry(final String taskToken, final String details,
            ActivityTypeExecutionOptions executionOptions) {
        SynchronousRetrier retrier = null;
        if (executionOptions != null) {
            ActivityTypeCompletionRetryOptions completionRetryOptions = executionOptions.getCompletionRetryOptions();
            retrier = createRetrier(completionRetryOptions);
        }
        if (retrier != null) {
            retrier.retry(new Runnable() {

                @Override
                public void run() {
                    respondActivityTaskCanceled(taskToken, details);
                }
            });
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
        SynchronousRetrier retrier = new SynchronousRetrier(retryParameters, UnknownResourceException.class);
        return retrier;
    }

    protected void respondActivityTaskCanceled(String taskToken, String details) {
        RespondActivityTaskCanceledRequest canceledResponse = new RespondActivityTaskCanceledRequest();
        canceledResponse.setTaskToken(taskToken);
        canceledResponse.setDetails(details);
        service.respondActivityTaskCanceled(canceledResponse);
    }

    protected void respondActivityTaskCompletedWithRetry(final String taskToken, final String output,
            ActivityTypeExecutionOptions executionOptions) {
        SynchronousRetrier retrier = null;
        if (executionOptions != null) {
            retrier = createRetrier(executionOptions.getCompletionRetryOptions());
        }
        if (retrier != null) {

            retrier.retry(new Runnable() {

                @Override
                public void run() {
                    respondActivityTaskCompleted(taskToken, output);
                }
            });
        }
        else {
            respondActivityTaskCompleted(taskToken, output);
        }
    }

    protected void respondActivityTaskCompleted(String taskToken, String output) {
        RespondActivityTaskCompletedRequest completedReponse = new RespondActivityTaskCompletedRequest();
        completedReponse.setTaskToken(taskToken);
        completedReponse.setResult(output);
        service.respondActivityTaskCompleted(completedReponse);
    }

    protected void checkRequiredProperty(Object value, String name) {
        if (value == null) {
            throw new IllegalStateException("required property " + name + " is not set");
        }
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void shutdownNow() {
    }

    @Override
    public boolean awaitTermination(long left, TimeUnit milliseconds) throws InterruptedException {
        //TODO: Waiting for all currently running pollAndProcessSingleTask to complete 
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.amazonaws.services.simpleworkflow.flow.worker.TaskPoller#suspend()
     */
    @Override
    public void suspend() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.amazonaws.services.simpleworkflow.flow.worker.TaskPoller#resume()
     */
    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSuspended() {
        return false;
    }
}
