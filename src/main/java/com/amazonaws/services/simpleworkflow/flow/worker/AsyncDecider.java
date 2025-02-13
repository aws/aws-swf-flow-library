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

import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.WorkflowException;
import com.amazonaws.services.simpleworkflow.flow.WorkflowExecutionLocal;
import com.amazonaws.services.simpleworkflow.flow.core.AsyncScope;
import com.amazonaws.services.simpleworkflow.flow.core.AsyncTaskInfo;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.StartTimerFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.TimerFiredEventAttributes;
import software.amazon.awssdk.services.swf.model.TimerStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionSignaledEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionStartedEventAttributes;

public class AsyncDecider {

    private static abstract class WorkflowAsyncScope extends AsyncScope {

        public WorkflowAsyncScope() {
            super(false, true);
        }

        public abstract Promise<String> getOutput();

    }

    private final class WorkflowExecuteAsyncScope extends WorkflowAsyncScope {

        private final WorkflowExecutionStartedEventAttributes attributes;

        private Promise<String> output;

        public WorkflowExecuteAsyncScope(HistoryEvent event) {
            assert event.eventTypeAsString().equals(EventType.WORKFLOW_EXECUTION_STARTED.toString());
            this.attributes = event.workflowExecutionStartedEventAttributes();
        }

        @Override
        protected void doAsync() throws Throwable {
            output = definition.execute(attributes.input());
        }

        @Override
        public Promise<String> getOutput() {
            return output;
        }

    }

    private final class UnhandledSignalAsyncScope extends WorkflowAsyncScope {

        private final Promise<String> output;

        private Throwable failure;

        private boolean cancellation;

        public UnhandledSignalAsyncScope(Promise<String> output, Throwable failure, boolean cancellation) {
            this.output = output;
            this.failure = failure;
            this.cancellation = cancellation;
        }

        @Override
        protected void doAsync() throws Throwable {
        }

        @Override
        public Promise<String> getOutput() {
            return output;
        }

        @Override
        public boolean isCancelRequested() {
            return super.isCancelRequested() || cancellation;
        }

        @Override
        public Throwable getFailure() {
            Throwable result = super.getFailure();
            if (failure != null) {
                result = failure;
            }
            return result;
        }

        @Override
        public boolean eventLoop() throws Throwable {
            boolean completed = super.eventLoop();
            if (completed && failure != null) {
                throw failure;
            }
            return completed;
        }

    }

    private static final Log log = LogFactory.getLog(AsyncDecider.class);

    private final WorkflowDefinitionFactory workflowDefinitionFactory;

    private WorkflowDefinition definition;

    @Setter
    @Getter
    private HistoryHelper historyHelper;

    @Getter
    private final DecisionsHelper decisionsHelper;

    private final GenericActivityClientImpl activityClient;

    private final GenericWorkflowClientImpl workflowClient;

    private final LambdaFunctionClientImpl lambdaFunctionClient;

    private final WorkflowClockImpl workflowClock;

    private final DecisionContext context;

    private WorkflowAsyncScope workflowAsyncScope;

    private boolean cancelRequested;

    private WorkflowContextImpl workflowContext;

    private boolean unhandledDecision;

    private boolean completed;

    private Throwable failure;

    @Getter
    private String originalTaskList;

    private Map<String, WorkflowExecutionLocal.Wrapper> savedWorkflowExecutionLocalValues;

    public AsyncDecider(WorkflowDefinitionFactory workflowDefinitionFactory, HistoryHelper historyHelper,
            DecisionsHelper decisionsHelper) throws Exception {
        this.workflowDefinitionFactory = workflowDefinitionFactory;
        this.historyHelper = historyHelper;
        this.decisionsHelper = decisionsHelper;
        this.activityClient = new GenericActivityClientImpl(decisionsHelper);
        PollForDecisionTaskResponse decisionTask = historyHelper.getDecisionTask();
        this.workflowClock = new WorkflowClockImpl(decisionsHelper);
        workflowContext = new WorkflowContextImpl(decisionTask, workflowClock);
        workflowContext.setComponentVersions(historyHelper.getComponentVersions());
        this.workflowClient = new GenericWorkflowClientImpl(decisionsHelper, workflowContext);
        this.lambdaFunctionClient = new LambdaFunctionClientImpl(decisionsHelper);
        context = new DecisionContextImpl(activityClient, workflowClient, workflowClock,
            workflowContext, lambdaFunctionClient);
    }

    public boolean isCancelRequested() {
        return cancelRequested;
    }

    public boolean hasCompletedWithoutUnhandledDecision() {
        return completed && !unhandledDecision;
    }

    private void handleWorkflowExecutionStarted(HistoryEvent event) {
        workflowAsyncScope = new WorkflowExecuteAsyncScope(event);
        originalTaskList = event.workflowExecutionStartedEventAttributes().taskList().name();
    }

    private void processEvent(HistoryEvent event, EventType eventType) throws Throwable {
        switch (eventType) {
        case ACTIVITY_TASK_CANCELED:
            activityClient.handleActivityTaskCanceled(event);
            break;
        case ACTIVITY_TASK_COMPLETED:
            activityClient.handleActivityTaskCompleted(event);
            break;
        case ACTIVITY_TASK_FAILED:
            activityClient.handleActivityTaskFailed(event);
            break;
        case ACTIVITY_TASK_STARTED:
            activityClient.handleActivityTaskStarted(event.activityTaskStartedEventAttributes());
            break;
        case ACTIVITY_TASK_TIMED_OUT:
            activityClient.handleActivityTaskTimedOut(event);
            break;
        case EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
            workflowClient.handleChildWorkflowExecutionCancelRequested(event);
            break;
        case CHILD_WORKFLOW_EXECUTION_CANCELED:
            workflowClient.handleChildWorkflowExecutionCanceled(event);
            break;
        case CHILD_WORKFLOW_EXECUTION_COMPLETED:
            workflowClient.handleChildWorkflowExecutionCompleted(event);
            break;
        case CHILD_WORKFLOW_EXECUTION_FAILED:
            workflowClient.handleChildWorkflowExecutionFailed(event);
            break;
        case CHILD_WORKFLOW_EXECUTION_STARTED:
            workflowClient.handleChildWorkflowExecutionStarted(event);
            break;
        case CHILD_WORKFLOW_EXECUTION_TERMINATED:
            workflowClient.handleChildWorkflowExecutionTerminated(event);
            break;
        case CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
            workflowClient.handleChildWorkflowExecutionTimedOut(event);
            break;
        case DECISION_TASK_COMPLETED:
            handleDecisionTaskCompleted(event);
            break;
        case DECISION_TASK_SCHEDULED:
            // NOOP
            break;
        case DECISION_TASK_STARTED:
            handleDecisionTaskStarted(event);
            break;
        case DECISION_TASK_TIMED_OUT:
            // Handled in the processEvent(event)
            break;
        case EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
            workflowClient.handleExternalWorkflowExecutionSignaled(event);
            break;
        case SCHEDULE_ACTIVITY_TASK_FAILED:
            activityClient.handleScheduleActivityTaskFailed(event);
            break;
        case SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
            workflowClient.handleSignalExternalWorkflowExecutionFailed(event);
            break;
        case START_CHILD_WORKFLOW_EXECUTION_FAILED:
            workflowClient.handleStartChildWorkflowExecutionFailed(event);
            break;
        case START_TIMER_FAILED:
            handleStartTimerFailed(event);
            break;
        case TIMER_FIRED:
            handleTimerFired(event);
            break;
        case WORKFLOW_EXECUTION_CANCEL_REQUESTED:
            handleWorkflowExecutionCancelRequested(event);
            break;
        case WORKFLOW_EXECUTION_SIGNALED:
            handleWorkflowExecutionSignaled(event);
            break;
        case WORKFLOW_EXECUTION_STARTED:
            handleWorkflowExecutionStarted(event);
            break;
        case WORKFLOW_EXECUTION_TERMINATED:
            // NOOP
            break;
        case WORKFLOW_EXECUTION_TIMED_OUT:
            // NOOP
            break;
        case ACTIVITY_TASK_SCHEDULED:
            decisionsHelper.handleActivityTaskScheduled(event);
            break;
        case ACTIVITY_TASK_CANCEL_REQUESTED:
            decisionsHelper.handleActivityTaskCancelRequested(event);
            break;
        case REQUEST_CANCEL_ACTIVITY_TASK_FAILED:
            decisionsHelper.handleRequestCancelActivityTaskFailed(event);
            break;
        case MARKER_RECORDED:
            break;
        case RECORD_MARKER_FAILED:
            break;
        case WORKFLOW_EXECUTION_COMPLETED:
            break;
        case COMPLETE_WORKFLOW_EXECUTION_FAILED:
            unhandledDecision = true;
            decisionsHelper.handleCompleteWorkflowExecutionFailed(event);
            break;
        case WORKFLOW_EXECUTION_FAILED:
            break;
        case FAIL_WORKFLOW_EXECUTION_FAILED:
            unhandledDecision = true;
            decisionsHelper.handleFailWorkflowExecutionFailed(event);
            break;
        case WORKFLOW_EXECUTION_CANCELED:
            break;
        case CANCEL_WORKFLOW_EXECUTION_FAILED:
            unhandledDecision = true;
            decisionsHelper.handleCancelWorkflowExecutionFailed(event);
            break;
        case WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
            break;
        case CONTINUE_AS_NEW_WORKFLOW_EXECUTION_FAILED:
            unhandledDecision = true;
            decisionsHelper.handleContinueAsNewWorkflowExecutionFailed(event);
            break;
        case TIMER_STARTED:
            handleTimerStarted(event);
            break;
        case TIMER_CANCELED:
            workflowClock.handleTimerCanceled(event);
            break;
        case LAMBDA_FUNCTION_SCHEDULED:
            decisionsHelper.handleLambdaFunctionScheduled(event);
            break;
        case LAMBDA_FUNCTION_STARTED:
            lambdaFunctionClient.handleLambdaFunctionStarted(event.lambdaFunctionStartedEventAttributes());
            break;
        case LAMBDA_FUNCTION_COMPLETED:
            lambdaFunctionClient.handleLambdaFunctionCompleted(event);
            break;
        case LAMBDA_FUNCTION_FAILED:
            lambdaFunctionClient.handleLambdaFunctionFailed(event);
            break;
        case LAMBDA_FUNCTION_TIMED_OUT:
            lambdaFunctionClient.handleLambdaFunctionTimedOut(event);
            break;
        case START_LAMBDA_FUNCTION_FAILED:
            lambdaFunctionClient.handleStartLambdaFunctionFailed(event);
            break;
        case SCHEDULE_LAMBDA_FUNCTION_FAILED:
            lambdaFunctionClient.handleScheduleLambdaFunctionFailed(event);
            break;
        case SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
            decisionsHelper.handleSignalExternalWorkflowExecutionInitiated(event);
            break;
        case REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
            decisionsHelper.handleRequestCancelExternalWorkflowExecutionInitiated(event);
            break;
        case REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
            decisionsHelper.handleRequestCancelExternalWorkflowExecutionFailed(event);
            break;
        case START_CHILD_WORKFLOW_EXECUTION_INITIATED:
            workflowClient.handleChildWorkflowExecutionInitiated(event);
            break;
        case CANCEL_TIMER_FAILED:
            decisionsHelper.handleCancelTimerFailed(event);
        }
    }

    private void eventLoop(HistoryEvent event) throws Throwable {
        if (completed) {
            return;
        }
        try {
            completed = workflowAsyncScope.eventLoop();
        } catch (CancellationException e) {
            if (!cancelRequested) {
                failure = e;
            }
            completed = true;
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            failure = e;
            completed = true;
        }
    }

    private void completeWorkflowIfCompleted() {
        if (hasCompletedWithoutUnhandledDecision()) {
            if (failure != null) {
                decisionsHelper.failWorkflowExecution(failure);
            } else if (cancelRequested) {
                decisionsHelper.cancelWorkflowExecution();
            } else {
                ContinueAsNewWorkflowExecutionParameters continueAsNewOnCompletion = workflowContext.getContinueAsNewOnCompletion();
                if (continueAsNewOnCompletion != null) {
                    decisionsHelper.continueAsNewWorkflowExecution(continueAsNewOnCompletion);
                } else {
                    Promise<String> output = workflowAsyncScope.getOutput();
                    if (output != null && output.isReady()) {
                        String workflowOutput = output.get();
                        decisionsHelper.completeWorkflowExecution(workflowOutput);
                    } else {
                        decisionsHelper.completeWorkflowExecution(null);
                    }
                }
            }
        }
    }

    private void handleDecisionTaskStarted(HistoryEvent event) throws Throwable {
    }

    private void handleWorkflowExecutionCancelRequested(HistoryEvent event) throws Throwable {
        workflowContext.setCancelRequested(true);
        workflowAsyncScope.cancel(new CancellationException());
        cancelRequested = true;
    }

    private void handleStartTimerFailed(HistoryEvent event) {
        StartTimerFailedEventAttributes attributes = event.startTimerFailedEventAttributes();
        String timerId = attributes.timerId();
        if (timerId.equals(DecisionsHelper.FORCE_IMMEDIATE_DECISION_TIMER)) {
            return;
        }
        workflowClock.handleStartTimerFailed(event);
    }

    private void handleTimerFired(HistoryEvent event) throws Throwable {
        TimerFiredEventAttributes attributes = event.timerFiredEventAttributes();
        String timerId = attributes.timerId();
        if (timerId.equals(DecisionsHelper.FORCE_IMMEDIATE_DECISION_TIMER)) {
            return;
        }
        workflowClock.handleTimerFired(event.eventId(), attributes);
    }

    private void handleTimerStarted(HistoryEvent event) {
        TimerStartedEventAttributes attributes = event.timerStartedEventAttributes();
        String timerId = attributes.timerId();
        if (timerId.equals(DecisionsHelper.FORCE_IMMEDIATE_DECISION_TIMER)) {
            return;
        }
        decisionsHelper.handleTimerStarted(event);
    }

    private void handleWorkflowExecutionSignaled(HistoryEvent event) throws Throwable {
        assert (event.eventTypeAsString().equals(EventType.WORKFLOW_EXECUTION_SIGNALED.toString()));
        final WorkflowExecutionSignaledEventAttributes signalAttributes = event.workflowExecutionSignaledEventAttributes();
        if (completed) {
            workflowAsyncScope = new UnhandledSignalAsyncScope(workflowAsyncScope.getOutput(), workflowAsyncScope.getFailure(),
                workflowAsyncScope.isCancelRequested());
            completed = false;
        }
        // This task is attached to the root context of the workflowAsyncScope
        new Task(workflowAsyncScope) {

            @Override
            protected void doExecute() throws Throwable {
                definition.signalRecieved(signalAttributes.signalName(), signalAttributes.input());
            }

        };
    }

    private void handleDecisionTaskCompleted(HistoryEvent event) {
        decisionsHelper.handleDecisionCompletion(event.decisionTaskCompletedEventAttributes());
    }

    public void decide() throws Exception {
        try {
            // Reusing the existing workflow definition for a cached decider
            if (definition != null) {
                CurrentDecisionContext.set(context);
                if (savedWorkflowExecutionLocalValues == null) {
                    ThreadLocalMetrics.getMetrics().recordCount(MetricName.AFFINITY_WORKER_WORKFLOW_EXECUTION_LOCAL_FAILURE.getName(), 1);
                    log.error("Unable to restore WorkflowExecutionLocals for affinity worker. Your WorkflowExecutionLocals will not work as expected.");
                } else {
                    WorkflowExecutionLocal.restoreFromSavedValues(savedWorkflowExecutionLocalValues);
                }
            } else {
                try {
                    definition = workflowDefinitionFactory.getWorkflowDefinition(context);
                } catch (Exception e) {
                    throw new Error("Failed to get workflow definition for " + context, e);
                }
            }
            if (definition == null) {
                final WorkflowType workflowType = context.getWorkflowContext().getWorkflowType();
                ThreadLocalMetrics.getMetrics().recordCount(MetricName.TYPE_NOT_FOUND.getName(), 1, MetricName.getWorkflowTypeDimension(workflowType));
            	throw new IllegalStateException("Null workflow definition returned for: " + workflowType);
            }
            decideImpl();
        } catch (AwsServiceException e) {
            // We don't want to fail workflow on service exceptions like 500 or throttling
            // Throwing from here drops decision task which is OK as it is rescheduled after its StartToClose timeout.
            if (e.isThrottlingException()) {
                if (log.isErrorEnabled()) {
                    log.error("Failing workflow " + workflowContext.getWorkflowExecution(), e);
                }
                decisionsHelper.failWorkflowDueToUnexpectedError(e);
            } else {
                throw e;
            }
        } catch (SdkClientException | Error e) {
            // Do not fail workflow on SdkClientException or Error. Fail the decision.
            throw e;
        } catch (Throwable e) {
            if (log.isErrorEnabled()) {
                log.error("Failing workflow " + workflowContext.getWorkflowExecution(), e);
            }
            decisionsHelper.failWorkflowDueToUnexpectedError(e);
        } finally {
            try {
                if (definition != null) {
                    decisionsHelper.setWorkflowContextData(definition.getWorkflowState());
                }
            } catch (WorkflowException e) {
                decisionsHelper.setWorkflowContextData(e.getDetails());
            } catch (Throwable e) {
                decisionsHelper.setWorkflowContextData(e.getMessage());
            }
            if (definition != null) {
                savedWorkflowExecutionLocalValues = WorkflowExecutionLocal.saveCurrentValues();
                workflowDefinitionFactory.deleteWorkflowDefinition(definition);
            }
        }
    }

    private void decideImpl() throws Throwable {
        List<HistoryEvent> singleDecisionEvents = historyHelper.getSingleDecisionEvents();
        while (singleDecisionEvents != null && singleDecisionEvents.size() > 0) {
            decisionsHelper.handleDecisionTaskStartedEvent();
            for (HistoryEvent event : singleDecisionEvents) {
                Long lastNonReplayedEventId = historyHelper.getLastNonReplayEventId();
                if (event.eventId() >= lastNonReplayedEventId) {
                    workflowClock.setReplaying(false);
                }
                long replayCurrentTimeMilliseconds = historyHelper.getReplayCurrentTimeMilliseconds();
                workflowClock.setReplayCurrentTimeMilliseconds(replayCurrentTimeMilliseconds);
                EventType eventType = EventType.fromValue(event.eventTypeAsString());
                processEvent(event, eventType);
                eventLoop(event);
            }
            completeWorkflowIfCompleted();
            singleDecisionEvents = historyHelper.getSingleDecisionEvents();
        }

        if (unhandledDecision) {
            unhandledDecision = false;
            completeWorkflowIfCompleted();
        }
    }

    public List<AsyncTaskInfo> getAsynchronousThreadDump() {
        checkAsynchronousThreadDumpState();
        return workflowAsyncScope.getAsynchronousThreadDump();
    }

    public String getAsynchronousThreadDumpAsString() {
        checkAsynchronousThreadDumpState();
        return workflowAsyncScope.getAsynchronousThreadDumpAsString();
    }

    private void checkAsynchronousThreadDumpState() {
        if (workflowAsyncScope == null) {
            throw new IllegalStateException("workflow hasn't started yet");
        }
        if (decisionsHelper.isWorkflowFailed()) {
            throw new IllegalStateException("Cannot get AsynchronousThreadDump of a failed workflow",
                decisionsHelper.getWorkflowFailureCause());
        }
    }

    public WorkflowDefinition getWorkflowDefinition() {
        return definition;
    }
}
