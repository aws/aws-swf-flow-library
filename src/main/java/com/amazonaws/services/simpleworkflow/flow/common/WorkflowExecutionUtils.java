/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.simpleworkflow.flow.common;

import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution.fromSdkType;
import static software.amazon.awssdk.services.swf.model.EventType.CONTINUE_AS_NEW_WORKFLOW_EXECUTION_FAILED;
import static software.amazon.awssdk.services.swf.model.EventType.RECORD_MARKER_FAILED;
import static software.amazon.awssdk.services.swf.model.EventType.SCHEDULE_ACTIVITY_TASK_FAILED;
import static software.amazon.awssdk.services.swf.model.EventType.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED;
import static software.amazon.awssdk.services.swf.model.EventType.START_CHILD_WORKFLOW_EXECUTION_FAILED;
import static software.amazon.awssdk.services.swf.model.EventType.START_TIMER_FAILED;
import static software.amazon.awssdk.services.swf.model.EventType.WORKFLOW_EXECUTION_CANCELED;
import static software.amazon.awssdk.services.swf.model.EventType.WORKFLOW_EXECUTION_COMPLETED;
import static software.amazon.awssdk.services.swf.model.EventType.WORKFLOW_EXECUTION_CONTINUED_AS_NEW;
import static software.amazon.awssdk.services.swf.model.EventType.WORKFLOW_EXECUTION_FAILED;
import static software.amazon.awssdk.services.swf.model.EventType.WORKFLOW_EXECUTION_TERMINATED;
import static software.amazon.awssdk.services.swf.model.EventType.WORKFLOW_EXECUTION_TIMED_OUT;

import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.CloseStatus;
import software.amazon.awssdk.services.swf.model.Decision;
import software.amazon.awssdk.services.swf.model.DescribeWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.DescribeWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.ExecutionStatus;
import software.amazon.awssdk.services.swf.model.GetWorkflowExecutionHistoryRequest;
import software.amazon.awssdk.services.swf.model.GetWorkflowExecutionHistoryResponse;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionContinuedAsNewEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;
import software.amazon.awssdk.utils.builder.CopyableBuilder;

/**
 * Convenience methods to be used by unit tests and during development.
 *
 * @author fateev
 */
public class WorkflowExecutionUtils {

    private static final String SDK_V2_SUFFIX_DECISION_ATTRIBUTE = "DecisionAttributes";

    /**
     * Blocks until workflow instance completes and returns its result. Useful
     * for unit tests and during development. <strong>Never</strong> use in
     * production setting as polling for worklow instance status is an expensive
     * operation.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @return workflow instance result.
     * @throws InterruptedException
     *             if thread is interrupted
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     */
    public static WorkflowExecutionCompletedEventAttributes waitForWorkflowExecutionResult(SwfClient service,
            String domain, WorkflowExecution workflowExecution) throws InterruptedException {
        return waitForWorkflowExecutionResult(service, domain, workflowExecution, null);
    }

    /**
     * Blocks until workflow instance completes and returns its result. Useful
     * for unit tests and during development. <strong>Never</strong> use in
     * production setting as polling for worklow instance status is an expensive
     * operation.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param config - SWF client configuration
     * @return workflow instance result.
     * @throws InterruptedException
     *             if thread is interrupted
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     */
    public static WorkflowExecutionCompletedEventAttributes waitForWorkflowExecutionResult(SwfClient service,
            String domain, WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) throws InterruptedException {
        try {
            return waitForWorkflowExecutionResult(service, domain, workflowExecution, 0, config);
        } catch (TimeoutException e) {
            throw new Error("should never happen", e);
        }
    }

    /**
     * Waits up to specified timeout until workflow instance completes and
     * returns its result. Useful for unit tests and during development.
     * <strong>Never</strong> use in production setting as polling for worklow
     * instance status is an expensive operation.
     *
     * @param service - SWF client
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param domain Registered Workflow domain
     * @param timeoutSeconds
     *            maximum time to wait for completion. 0 means wait forever.
     * @return workflow instance result.
     * @throws InterruptedException
     *             if thread is interrupted
     * @throws TimeoutException
     *             if instance is not complete after specified timeout
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     */
    public static WorkflowExecutionCompletedEventAttributes waitForWorkflowExecutionResult(SwfClient service,
            String domain, WorkflowExecution workflowExecution, long timeoutSeconds)
            throws InterruptedException, TimeoutException {
        return waitForWorkflowExecutionResult(service, domain, workflowExecution, timeoutSeconds, null);
    }

    /**
     * Blocks until workflow instance completes and returns its result. Useful
     * for unit tests and during development. <strong>Never</strong> use in
     * production setting as polling for worklow instance status is an expensive
     * operation.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param config - SWF client configuration
     * @param timeoutSeconds
     *            maximum time to wait for completion. 0 means wait forever.
     * @return workflow instance result.
     * @throws InterruptedException
     *             if thread is interrupted
     * @throws TimeoutException
     *             if instance is not complete after specified timeout
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     */
    public static WorkflowExecutionCompletedEventAttributes waitForWorkflowExecutionResult(SwfClient service,
           String domain, WorkflowExecution workflowExecution, long timeoutSeconds, SimpleWorkflowClientConfig config)
            throws InterruptedException, TimeoutException {
        if (!waitForWorkflowInstanceCompletion(service, domain, workflowExecution, timeoutSeconds, config).equals(
                CloseStatus.COMPLETED.toString())) {
            String historyDump = WorkflowExecutionUtils.prettyPrintHistory(service, domain, workflowExecution, config);
            throw new RuntimeException("Workflow instance is not in completed state:\n" + historyDump);
        }
        return getWorkflowExecutionResult(service, domain, workflowExecution, config);
    }

    /**
     * Blocks until workflow instance completes and returns its result. Useful
     * for unit tests and during development. <strong>Never</strong> use in
     * production setting as polling for worklow instance status is an expensive
     * operation.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @return workflow instance result.
     * @throws IllegalStateException
     *             if workflow is still running
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     */
    public static WorkflowExecutionCompletedEventAttributes getWorkflowExecutionResult(SwfClient service,
            String domain, WorkflowExecution workflowExecution) {
        return getWorkflowExecutionResult(service, domain, workflowExecution, null);
    }

    /**
     * Blocks until workflow instance completes and returns its result. Useful
     * for unit tests and during development. <strong>Never</strong> use in
     * production setting as polling for worklow instance status is an expensive
     * operation.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param config - SWF client configuration
     * @return workflow instance result.
     * @throws IllegalStateException
     *             if workflow is still running
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     */
    public static WorkflowExecutionCompletedEventAttributes getWorkflowExecutionResult(SwfClient service,
           String domain, WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) {
        HistoryEvent closeEvent = getInstanceCloseEvent(service, domain, workflowExecution, config);
        if (closeEvent == null) {
            throw new IllegalStateException("Workflow is still running");
        }
        if (closeEvent.eventTypeAsString().equals(WORKFLOW_EXECUTION_COMPLETED.toString())) {
            return closeEvent.workflowExecutionCompletedEventAttributes();
        }
        throw new RuntimeException("Workflow end state is not completed: " + prettyPrintHistoryEvent(closeEvent));
    }

    public static HistoryEvent getInstanceCloseEvent(SwfClient service, String domain,
            WorkflowExecution workflowExecution) {
        return getInstanceCloseEvent(service, domain, workflowExecution, null);
    }

    public static HistoryEvent getInstanceCloseEvent(SwfClient service, String domain,
             WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) {

        WorkflowExecutionInfo executionInfo = describeWorkflowInstance(service, domain, workflowExecution, config);
        if (executionInfo == null || executionInfo.executionStatusAsString().equals(ExecutionStatus.OPEN.toString())) {
            return null;
        }

        List<HistoryEvent> events = getHistory(service, domain, workflowExecution, true, config);
        if (events.size() == 0) {
            throw new IllegalStateException("empty history");
        }

        HistoryEvent last = events.get(0);
        if (!isWorkflowExecutionCompletedEvent(last)) {
            throw new IllegalStateException("unexpected last history event for workflow in " + executionInfo.executionStatusAsString()
                    + " state: " + last.eventType());
        }
        return last;
    }

    public static boolean isWorkflowExecutionCompletedEvent(HistoryEvent event) {
        return ((event != null) && (event.eventTypeAsString().equals(WORKFLOW_EXECUTION_COMPLETED.toString())
                || event.eventTypeAsString().equals(WORKFLOW_EXECUTION_CANCELED.toString())
                || event.eventTypeAsString().equals(WORKFLOW_EXECUTION_FAILED.toString())
                || event.eventTypeAsString().equals(WORKFLOW_EXECUTION_TIMED_OUT.toString())
                || event.eventTypeAsString().equals(WORKFLOW_EXECUTION_CONTINUED_AS_NEW.toString()) || event.eventTypeAsString().equals(
                WORKFLOW_EXECUTION_TERMINATED.toString())));
    }

    public static boolean isActivityTaskClosedEvent(HistoryEvent event) {
        return ((event != null) && (event.eventTypeAsString().equals(EventType.ACTIVITY_TASK_COMPLETED.toString())
                || event.eventTypeAsString().equals(EventType.ACTIVITY_TASK_CANCELED.toString())
                || event.eventTypeAsString().equals(EventType.ACTIVITY_TASK_FAILED.toString()) || event.eventTypeAsString().equals(
                EventType.ACTIVITY_TASK_TIMED_OUT.toString())));
    }

    public static boolean isExternalWorkflowClosedEvent(HistoryEvent event) {
        return ((event != null) && (event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_COMPLETED.toString())
                || event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_CANCELED.toString())
                || event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_FAILED.toString())
                || event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_TERMINATED.toString()) || event.eventTypeAsString().equals(
                EventType.CHILD_WORKFLOW_EXECUTION_TIMED_OUT.toString())));
    }

    public static WorkflowExecution getWorkflowIdFromExternalWorkflowCompletedEvent(HistoryEvent event) {
        if (event != null) {
            software.amazon.awssdk.services.swf.model.WorkflowExecution sdkv2Execution = null;
            if (event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_COMPLETED.toString())) {
                sdkv2Execution = event.childWorkflowExecutionCompletedEventAttributes().workflowExecution();
            } else if (event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_CANCELED.toString())) {
                sdkv2Execution = event.childWorkflowExecutionCanceledEventAttributes().workflowExecution();
            } else if (event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_FAILED.toString())) {
                sdkv2Execution = event.childWorkflowExecutionFailedEventAttributes().workflowExecution();
            } else if (event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_TERMINATED.toString())) {
                sdkv2Execution = event.childWorkflowExecutionTerminatedEventAttributes().workflowExecution();
            } else if (event.eventTypeAsString().equals(EventType.CHILD_WORKFLOW_EXECUTION_TIMED_OUT.toString())) {
                sdkv2Execution = event.childWorkflowExecutionTimedOutEventAttributes().workflowExecution();
            }
            return fromSdkType(sdkv2Execution);
        }

        return null;
    }

    public static String getId(HistoryEvent historyEvent) {
        String id = null;
        if (historyEvent != null) {
            if (historyEvent.eventTypeAsString().equals(START_CHILD_WORKFLOW_EXECUTION_FAILED.toString())) {
                id = historyEvent.startChildWorkflowExecutionFailedEventAttributes().workflowId();
            } else if (historyEvent.eventTypeAsString().equals(SCHEDULE_ACTIVITY_TASK_FAILED.toString())) {
                id = historyEvent.scheduleActivityTaskFailedEventAttributes().activityId();
            } else if (historyEvent.eventTypeAsString().equals(START_TIMER_FAILED.toString())) {
                id = historyEvent.startTimerFailedEventAttributes().timerId();
            }
        }

        return id;
    }

    public static String getFailureCause(HistoryEvent historyEvent) {
        String failureCause = null;
        if (historyEvent != null) {
            if (historyEvent.eventTypeAsString().equals(START_CHILD_WORKFLOW_EXECUTION_FAILED.toString())) {
                failureCause = historyEvent.startChildWorkflowExecutionFailedEventAttributes().causeAsString();
            } else if (historyEvent.eventTypeAsString().equals(SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED.toString())) {
                failureCause = historyEvent.signalExternalWorkflowExecutionFailedEventAttributes().causeAsString();
            } else if (historyEvent.eventTypeAsString().equals(SCHEDULE_ACTIVITY_TASK_FAILED.toString())) {
                failureCause = historyEvent.scheduleActivityTaskFailedEventAttributes().causeAsString();
            } else if (historyEvent.eventTypeAsString().equals(START_TIMER_FAILED.toString())) {
                failureCause = historyEvent.startTimerFailedEventAttributes().causeAsString();
            } else if (historyEvent.eventTypeAsString().equals(CONTINUE_AS_NEW_WORKFLOW_EXECUTION_FAILED.toString())) {
                failureCause = historyEvent.continueAsNewWorkflowExecutionFailedEventAttributes().causeAsString();
            } else if (historyEvent.eventTypeAsString().equals(RECORD_MARKER_FAILED.toString())) {
                failureCause = historyEvent.recordMarkerFailedEventAttributes().causeAsString();
            }
        }

        return failureCause;
    }

    /**
     * Blocks until workflow instance completes. <strong>Never</strong> use in
     * production setting as polling for worklow instance status is an expensive
     * operation.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @return instance close status
     * @throws InterruptedException
     *             if thread is interrupted
     */
    public static String waitForWorkflowInstanceCompletion(SwfClient service, String domain,
            WorkflowExecution workflowExecution) throws InterruptedException {
        return waitForWorkflowInstanceCompletion(service, domain, workflowExecution, null);
    }

    public static String waitForWorkflowInstanceCompletion(SwfClient service, String domain,
               WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) throws InterruptedException {
        try {
            return waitForWorkflowInstanceCompletion(service, domain, workflowExecution, 0, config);
        } catch (TimeoutException e) {
            throw new Error("should never happen", e);
        }
    }

    /**
     * Waits up to specified timeout for workflow instance completion.
     * <strong>Never</strong> use in production setting as polling for worklow
     * instance status is an expensive operation.
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param timeoutSeconds
     *            maximum time to wait for completion. 0 means wait forever.
     * @return instance close status
     * @throws InterruptedException
     *             if thread is interrupted
     * @throws TimeoutException
     *             if instance is not complete after specified timeout
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     */
    public static String waitForWorkflowInstanceCompletion(SwfClient service, String domain,
            WorkflowExecution workflowExecution, long timeoutSeconds)
                throws InterruptedException, TimeoutException {
        return waitForWorkflowInstanceCompletion(service, domain, workflowExecution, timeoutSeconds, null);
    }

    /**
     * Waits up to specified timeout for workflow instance completion.
     * <strong>Never</strong> use in production setting as polling for worklow
     * instance status is an expensive operation.
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param timeoutSeconds
     *            maximum time to wait for completion. 0 means wait forever.
     * @param config - SWF client configuration
     * @return instance close status
     * @throws InterruptedException
     *             if thread is interrupted
     * @throws TimeoutException
     *             if instance is not complete after specified timeout
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     */
    public static String waitForWorkflowInstanceCompletion(SwfClient service, String domain,
           WorkflowExecution workflowExecution, long timeoutSeconds, SimpleWorkflowClientConfig config)
            throws InterruptedException, TimeoutException {
        long start = System.currentTimeMillis();
        WorkflowExecutionInfo executionInfo = null;
        do {
            if (timeoutSeconds > 0 && System.currentTimeMillis() - start >= timeoutSeconds * 1000) {
                String historyDump = WorkflowExecutionUtils.prettyPrintHistory(service, domain, workflowExecution, config);
                throw new TimeoutException("Workflow instance is not complete after " + timeoutSeconds + " seconds: \n"
                        + historyDump);
            }
            if (executionInfo != null) {
                Thread.sleep(1000);
            }
            executionInfo = describeWorkflowInstance(service, domain, workflowExecution, config);
        }
        while (executionInfo.executionStatusAsString().equals(ExecutionStatus.OPEN.toString()));
        return executionInfo.closeStatusAsString();
    }

    /**
     * Like
     * {@link #waitForWorkflowInstanceCompletion(SwfClient, String, WorkflowExecution, long)},
     * except will wait for continued generations of the original workflow
     * execution too.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param timeoutSeconds
     *            maximum time to wait for completion. 0 means wait forever.
     * @return last workflow instance close status
     * @throws InterruptedException
     *             if thread is interrupted
     * @throws TimeoutException
     *             if instance is not complete after specified timeout
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     * @see #waitForWorkflowInstanceCompletion(SwfClient, String,
     *      WorkflowExecution, long)
     */
    public static String waitForWorkflowInstanceCompletionAcrossGenerations(SwfClient service, String domain,
            WorkflowExecution workflowExecution, long timeoutSeconds) throws InterruptedException, TimeoutException {
        return waitForWorkflowInstanceCompletionAcrossGenerations(service, domain, workflowExecution, timeoutSeconds, null);
    }


    /**
     * Like
     * {@link #waitForWorkflowInstanceCompletion(SwfClient, String, WorkflowExecution, long)},
     * except will wait for continued generations of the original workflow
     * execution too.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param timeoutSeconds
     *            maximum time to wait for completion. 0 means wait forever.
     * @param config - SWF client configuration
     * @return last workflow instance close status
     * @throws InterruptedException
     *             if thread is interrupted
     * @throws TimeoutException
     *             if instance is not complete after specified timeout
     * @throws RuntimeException
     *             if workflow instance ended up in any state but completed
     * @see #waitForWorkflowInstanceCompletion(SwfClient, String,
     *      WorkflowExecution, long)
     */
    public static String waitForWorkflowInstanceCompletionAcrossGenerations(SwfClient service, String domain,
                WorkflowExecution workflowExecution, long timeoutSeconds, SimpleWorkflowClientConfig config) throws InterruptedException, TimeoutException {

        WorkflowExecution lastExecutionToRun = workflowExecution;
        long millisecondsAtFirstWait = System.currentTimeMillis();
        String lastExecutionToRunCloseStatus = waitForWorkflowInstanceCompletion(service, domain, lastExecutionToRun,
                timeoutSeconds, config);

        // keep waiting if the instance continued as new
        while (lastExecutionToRunCloseStatus.equals(CloseStatus.CONTINUED_AS_NEW.toString())) {
            // get the new execution's information
            HistoryEvent closeEvent = getInstanceCloseEvent(service, domain, lastExecutionToRun, config);
            WorkflowExecutionContinuedAsNewEventAttributes continuedAsNewAttributes = closeEvent.workflowExecutionContinuedAsNewEventAttributes();

            WorkflowExecution newGenerationExecution = WorkflowExecution.builder().workflowId(lastExecutionToRun.getWorkflowId())
                .runId(continuedAsNewAttributes.newExecutionRunId()).build();

            // and wait for it
            long currentTime = System.currentTimeMillis();
            long millisecondsSinceFirstWait = currentTime - millisecondsAtFirstWait;
            long timeoutInSecondsForNextWait = timeoutSeconds - (millisecondsSinceFirstWait / 1000L);

            lastExecutionToRunCloseStatus = waitForWorkflowInstanceCompletion(service, domain, newGenerationExecution, timeoutInSecondsForNextWait, config);
            lastExecutionToRun = newGenerationExecution;
        }

        return lastExecutionToRunCloseStatus;
    }


    /**
     * Like
     * {@link #waitForWorkflowInstanceCompletion(SwfClient, String, WorkflowExecution, long)},
     * except will wait for continued generations of the original workflow
     * execution too.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @return last workflow instance close status
     * @throws InterruptedException
     *             if thread is interrupted
     * @see #waitForWorkflowInstanceCompletion(SwfClient, String,
     *      WorkflowExecution, long)
     */
    public static String waitForWorkflowInstanceCompletionAcrossGenerations(SwfClient service, String domain,
            WorkflowExecution workflowExecution) throws InterruptedException {
        try {
            return waitForWorkflowInstanceCompletionAcrossGenerations(service, domain, workflowExecution, 0L, null);
        } catch (TimeoutException e) {
            throw new Error("should never happen", e);
        }
    }

    public static WorkflowExecutionInfo describeWorkflowInstance(SwfClient service, String domain,
            WorkflowExecution workflowExecution) {
        return describeWorkflowInstance(service, domain, workflowExecution, null);
    }

    public static WorkflowExecutionInfo describeWorkflowInstance(SwfClient service, String domain,
                                                                 WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) {
        DescribeWorkflowExecutionRequest describeRequest = DescribeWorkflowExecutionRequest.builder().domain(domain)
            .execution(workflowExecution.toSdkType()).build();

        describeRequest = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(describeRequest, config);
        DescribeWorkflowExecutionResponse executionDetail = service.describeWorkflowExecution(describeRequest);
        return executionDetail.executionInfo();
    }


    /**
     * Returns workflow instance history in a human readable format.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @return A formatted string containing the workflow execution history with workflow tasks included
     */
    public static String prettyPrintHistory(SwfClient service, String domain, WorkflowExecution workflowExecution) {
        return prettyPrintHistory(service, domain, workflowExecution, null);
    }

    /**
     * Returns workflow instance history in a human readable format.
     * It includes workflow tasks events(decider events) by default
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param config - SWF client configuration
     * @return A formatted string containing the workflow execution history with workflow tasks included
     */
    public static String prettyPrintHistory(SwfClient service, String domain, WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) {
        return prettyPrintHistory(service, domain, workflowExecution, true, config);
    }

    /**
     * Returns workflow instance history in a human readable format.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param showWorkflowTasks
     *            when set to false workflow task events (decider events) are
     *            not included
     * @return A formatted string containing the workflow execution history
     */
    public static String prettyPrintHistory(SwfClient service, String domain, WorkflowExecution workflowExecution,
            boolean showWorkflowTasks) {
        return prettyPrintHistory(service, domain, workflowExecution, showWorkflowTasks, null);
    }

    /**
     * Returns workflow instance history in a human readable format.
     *
     * @param service - SWF client
     * @param domain Registered Workflow domain
     * @param workflowExecution
     *            result of
     *            {@link SwfClient#startWorkflowExecution(StartWorkflowExecutionRequest)}
     * @param config - SWF client configuration
     * @param showWorkflowTasks
     *            when set to false workflow task events (decider events) are
     *            not included
     * @return A formatted string containing the workflow execution history
     */
    public static String prettyPrintHistory(SwfClient service, String domain, WorkflowExecution workflowExecution,
            boolean showWorkflowTasks, SimpleWorkflowClientConfig config) {
        List<HistoryEvent> events = getHistory(service, domain, workflowExecution, config);
        return prettyPrintHistory(events, showWorkflowTasks);
    }


    public static List<HistoryEvent> getHistory(SwfClient service, String domain, WorkflowExecution workflowExecution) {
        return getHistory(service, domain, workflowExecution, false, null);
    }

    public static List<HistoryEvent> getHistory(SwfClient service, String domain, WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) {
        return getHistory(service, domain, workflowExecution, false, config);
    }

    public static List<HistoryEvent> getHistory(SwfClient service, String domain, WorkflowExecution workflowExecution,
            boolean reverseOrder) {
        return getHistory(service, domain, workflowExecution, reverseOrder, null);
    }

    public static List<HistoryEvent> getHistory(SwfClient service, String domain, WorkflowExecution workflowExecution,
            boolean reverseOrder, SimpleWorkflowClientConfig config) {
        List<HistoryEvent> events = new ArrayList<HistoryEvent>();
        String nextPageToken = null;
        do {
            GetWorkflowExecutionHistoryResponse history = getHistoryPage(nextPageToken, service, domain, workflowExecution, reverseOrder, config);
            events.addAll(history.events());
            nextPageToken = history.nextPageToken();
        }
        while (nextPageToken != null);
        return events;
    }

    public static GetWorkflowExecutionHistoryResponse getHistoryPage(String nextPageToken, SwfClient service, String domain,
        WorkflowExecution workflowExecution) {
        return getHistoryPage(nextPageToken, service, domain, workflowExecution, false, null);
    }

    public static GetWorkflowExecutionHistoryResponse getHistoryPage(String nextPageToken, SwfClient service, String domain,
            WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) {
        return getHistoryPage(nextPageToken, service, domain, workflowExecution, false, config);
    }

    public static GetWorkflowExecutionHistoryResponse getHistoryPage(String nextPageToken, SwfClient service, String domain,
            WorkflowExecution workflowExecution, boolean reverseOrder) {
        return getHistoryPage(nextPageToken, service, domain, workflowExecution, reverseOrder, null);
    }

    public static GetWorkflowExecutionHistoryResponse getHistoryPage(String nextPageToken, SwfClient service, String domain,
                                         WorkflowExecution workflowExecution, boolean reverseOrder, SimpleWorkflowClientConfig config) {
        GetWorkflowExecutionHistoryRequest getHistoryRequest = GetWorkflowExecutionHistoryRequest.builder()
            .domain(domain)
            .execution(workflowExecution.toSdkType())
            .reverseOrder(reverseOrder)
            .nextPageToken(nextPageToken).build();

        getHistoryRequest = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(getHistoryRequest, config);
        GetWorkflowExecutionHistoryRequest finalGetHistoryRequest = getHistoryRequest;
        GetWorkflowExecutionHistoryResponse history = ThreadLocalMetrics.getMetrics().recordSupplier(() -> service.getWorkflowExecutionHistory(
            finalGetHistoryRequest), MetricName.Operation.GET_WORKFLOW_EXECUTION_HISTORY.getName(), TimeUnit.MILLISECONDS);
        if (history == null) {
            throw new IllegalArgumentException("unknown workflow execution: " + workflowExecution);
        }
        return history;
    }

    /**
     * Returns workflow instance history in a human readable format.
     *
     * @param history Workflow instance history
     * @param showWorkflowTasks
     *            when set to false workflow task events (decider events) are
     *            not included
     * @return A formatted string containing the workflow execution history
     */
    public static String prettyPrintHistory(GetWorkflowExecutionHistoryResponse history, boolean showWorkflowTasks) {
        return prettyPrintHistory(history.events(), showWorkflowTasks);
    }

    /**
     * Returns workflow instance history in a human readable format.
     *
     * @param events Workflow instance history events
     * @param showWorkflowTasks
     *            when set to false workflow task events (decider events) are
     *            not included
     * @return A formatted string containing the workflow execution history
     */
    public static String prettyPrintHistory(Iterable<HistoryEvent> events, boolean showWorkflowTasks) {
        StringBuffer result = new StringBuffer();
        result.append("{");
        boolean first = true;
        for (HistoryEvent event : events) {
            if (!showWorkflowTasks && event.eventTypeAsString().startsWith("WorkflowTask")) {
                continue;
            }
            if (first) {
                first = false;
            } else {
                result.append(",");
            }
            result.append("\n    ");
            result.append(prettyPrintHistoryEvent(event));
        }
        result.append("\n}");
        return result.toString();
    }

    /**
     * Returns decision events in a human readable format
     *
     * @param decisions
     *            decisions to pretty print
     * @return A formatted string containing a JSON-like representation of the decisions,
     *         with each decision on a new line enclosed in curly braces
     */
    public static String prettyPrintDecisions(Iterable<Decision> decisions) {
        StringBuffer result = new StringBuffer();
        result.append("{");
        boolean first = true;
        for (Decision decision : decisions) {
            if (first) {
                first = false;
            } else {
                result.append(",");
            }
            result.append("\n    ");
            result.append(prettyPrintDecision(decision));
        }
        result.append("\n}");
        return result.toString();
    }

    /**
     * Returns single event in a human readable format
     *
     * @param event
     *            event to pretty print
     * @return A formatted string containing a JSON-like representation of the decisions,
     *         with each decision on a new line enclosed in curly braces
     */
    public static String prettyPrintHistoryEvent(HistoryEvent event) {
        String eventType = event.eventTypeAsString();
        StringBuffer result = new StringBuffer();
        result.append(eventType);
        result.append(prettyPrintObject(event, "getType", true, "    ", false));
        return result.toString();
    }

    /**
     * Returns single decision in a human readable format
     *
     * @param decision
     *            event to pretty print
     *
     * @return A formatted string containing a JSON-like representation of a single decision
     */
    public static String prettyPrintDecision(Decision decision) {
        return prettyPrintObject(decision, "getDecisionType", true, "", true);
    }

    /**
     * Not really a generic method for printing random object graphs. But it
     * works for events and decisions.
     *
     * @param object The object to be pretty printed
     * @param methodToSkip Name of the method to be skipped during processing
     * @param skipNullsAndEmptyCollections If true, null values and empty collections will be excluded from output
     * @param indentation The string to use for indentation in the formatted output
     * @param skipLevel If true, skips adding enclosing braces and additional formatting levels
     *
     * @return A formatted string containing a JSON-like representation of the event,
     *         with each event on a new line enclosed in curly braces
     */
    private static String prettyPrintObject(Object object, String methodToSkip, boolean skipNullsAndEmptyCollections,
            String indentation, boolean skipLevel) {
        StringBuffer result = new StringBuffer();
        if (object == null) {
            return "null";
        }
        Class<? extends Object> clz = object.getClass();
        if (Number.class.isAssignableFrom(clz)) {
            return String.valueOf(object);
        }
        if (Boolean.class.isAssignableFrom(clz)) {
            return String.valueOf(object);
        }
        if (clz.equals(String.class)) {
            return (String) object;
        }
        if (clz.equals(Date.class) || clz.equals(Instant.class)) {
            return String.valueOf(object);
        }
        if (Map.class.isAssignableFrom(clz)) {
            return String.valueOf(object);
        }
        if (Collection.class.isAssignableFrom(clz)) {
            return String.valueOf(object);
        }

        if (!skipLevel) {
            result.append(" {");
        }
        if (object instanceof Decision.Builder) {
            result = new StringBuffer();
        }
        Method[] eventMethods = object.getClass().getMethods();
        Arrays.sort(eventMethods, Comparator.comparing(Method::getName));
        boolean first = true;
        for (Method method : eventMethods) {
            String name = method.getName();
            boolean isNotBuilder = !name.contains("toBuilder") || !method.getReturnType().equals(
                CopyableBuilder.class); //ignore methods other than builder in AWSSDK-v2
            if ((!name.startsWith("get") || name.startsWith("getValueForField")) && isNotBuilder) {
                continue;
            }
            if (name.equals(methodToSkip) || name.equals("getClass")) {
                continue;
            }
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            Object value;
            try {
                if (!method.isAccessible()) {
                    method.setAccessible(true);
                }
                value = method.invoke(object, (Object[]) null);
                if (value != null && value.getClass().equals(String.class) && name.equals("getDetails")) {
                    value = printDetails((String) value);
                }
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e.getTargetException());
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (skipNullsAndEmptyCollections) {
                if (value == null) {
                    continue;
                }
                if (value instanceof Map && ((Map<?, ?>) value).isEmpty()) {
                    continue;
                }
                if (value instanceof Collection && ((Collection<?>) value).isEmpty()) {
                    continue;
                }
            }
            if (name.contains("toBuilder")) {
                skipLevel = true; //skip appending because its a toBuilder() method
                result = new StringBuffer(); //remove the " {" appended at L602
            }

            if (!skipLevel) {
                if (first) {
                    first = false;
                } else {
                    result.append(";");
                }
                name = name.substring(3);
                if (!name.endsWith(SDK_V2_SUFFIX_DECISION_ATTRIBUTE)) {
                    result.append("\n");
                    result.append(indentation);
                    result.append("    ");
                    result.append(name);
                    result.append(" = ");
                } else {
                    result.trimToSize();
                    result.append(indentation);
                    result.append(name, 0, name.length()
                        - SDK_V2_SUFFIX_DECISION_ATTRIBUTE.length()).append(" ");
                    skipLevel  = true; //skip end brackets of in case of Decision attribute
                    //remove SDK Decision attribute as well for decisions events
                }
                result.append(prettyPrintObject(value, methodToSkip, skipNullsAndEmptyCollections, indentation + "    ", false));
            } else {
                result.append(prettyPrintObject(value, methodToSkip, skipNullsAndEmptyCollections, indentation, false));
            }
        }

        if (!skipLevel) {
            result.append("\n");
            result.append(indentation);
            result.append("}");
        }
        return result.toString();
    }

    public static String printDetails(String details) {
        Throwable failure = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.activateDefaultTyping(
                    BasicPolymorphicTypeValidator.builder()
                            .allowIfBaseType(Object.class)
                            .build(),
                    DefaultTyping.NON_FINAL);

            failure = mapper.readValue(details, Throwable.class);
        } catch (Exception e) {
            // eat up any data converter exceptions
        }

        if (failure != null) {
            StringBuilder builder = new StringBuilder();

            // Also print callstack
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            failure.printStackTrace(pw);

            builder.append(sw.toString());

            details = builder.toString();
        }

        return details;
    }

    /**
     * Simple Workflow limits length of the reason field. This method truncates
     * the passed argument to the maximum length.
     *
     * @param reason
     *            string value to truncate
     * @return truncated value
     */
    public static String truncateReason(String reason) {
        if (reason != null && reason.length() > FlowValueConstraint.FAILURE_REASON.getMaxSize()) {
            reason = reason.substring(0, FlowValueConstraint.FAILURE_REASON.getMaxSize());
        }
        return reason;
    }

    public static String truncateDetails(String details) {
        if (details != null && details.length() > FlowValueConstraint.FAILURE_DETAILS.getMaxSize()) {
            details = details.substring(0, FlowValueConstraint.FAILURE_DETAILS.getMaxSize());
        }
        return details;
    }

    public static Throwable truncateStackTrace(Throwable throwable) {
        Throwable cause = throwable;
        while (cause != null) {
            cause.setStackTrace(new StackTraceElement[0]);
            cause = cause.getCause();
        }
        return throwable;
    }

}
