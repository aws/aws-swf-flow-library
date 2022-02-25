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

import java.util.List;
import java.util.concurrent.CancellationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.WorkflowException;
import com.amazonaws.services.simpleworkflow.flow.core.AsyncScope;
import com.amazonaws.services.simpleworkflow.flow.core.AsyncTaskInfo;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.EventType;
import com.amazonaws.services.simpleworkflow.model.HistoryEvent;
import com.amazonaws.services.simpleworkflow.model.StartTimerFailedEventAttributes;
import com.amazonaws.services.simpleworkflow.model.TimerFiredEventAttributes;
import com.amazonaws.services.simpleworkflow.model.TimerStartedEventAttributes;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionSignaledEventAttributes;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionStartedEventAttributes;

class AsyncDecider {

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
            assert event.getEventType().equals(EventType.WorkflowExecutionStarted.toString());
            this.attributes = event.getWorkflowExecutionStartedEventAttributes();
        }

        @Override
        protected void doAsync() throws Throwable {
            output = definition.execute(attributes.getInput());
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

    private final HistoryHelper historyHelper;

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

    public AsyncDecider(WorkflowDefinitionFactory workflowDefinitionFactory, HistoryHelper historyHelper,
            DecisionsHelper decisionsHelper) throws Exception {
        this.workflowDefinitionFactory = workflowDefinitionFactory;
        this.historyHelper = historyHelper;
        this.decisionsHelper = decisionsHelper;
        this.activityClient = new GenericActivityClientImpl(decisionsHelper);
        DecisionTask decisionTask = historyHelper.getDecisionTask();
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

    private void handleWorkflowExecutionStarted(HistoryEvent event) {
        workflowAsyncScope = new WorkflowExecuteAsyncScope(event);
    }

    private void processEvent(HistoryEvent event, EventType eventType) throws Throwable {
        switch (eventType) {
        case ActivityTaskCanceled:
            activityClient.handleActivityTaskCanceled(event);
            break;
        case ActivityTaskCompleted:
            activityClient.handleActivityTaskCompleted(event);
            break;
        case ActivityTaskFailed:
            activityClient.handleActivityTaskFailed(event);
            break;
        case ActivityTaskStarted:
            activityClient.handleActivityTaskStarted(event.getActivityTaskStartedEventAttributes());
            break;
        case ActivityTaskTimedOut:
            activityClient.handleActivityTaskTimedOut(event);
            break;
        case ExternalWorkflowExecutionCancelRequested:
            workflowClient.handleChildWorkflowExecutionCancelRequested(event);
            break;
        case ChildWorkflowExecutionCanceled:
            workflowClient.handleChildWorkflowExecutionCanceled(event);
            break;
        case ChildWorkflowExecutionCompleted:
            workflowClient.handleChildWorkflowExecutionCompleted(event);
            break;
        case ChildWorkflowExecutionFailed:
            workflowClient.handleChildWorkflowExecutionFailed(event);
            break;
        case ChildWorkflowExecutionStarted:
            workflowClient.handleChildWorkflowExecutionStarted(event);
            break;
        case ChildWorkflowExecutionTerminated:
            workflowClient.handleChildWorkflowExecutionTerminated(event);
            break;
        case ChildWorkflowExecutionTimedOut:
            workflowClient.handleChildWorkflowExecutionTimedOut(event);
            break;
        case DecisionTaskCompleted:
            handleDecisionTaskCompleted(event);
            break;
        case DecisionTaskScheduled:
            // NOOP
            break;
        case DecisionTaskStarted:
            handleDecisionTaskStarted(event);
            break;
        case DecisionTaskTimedOut:
            // Handled in the processEvent(event)
            break;
        case ExternalWorkflowExecutionSignaled:
            workflowClient.handleExternalWorkflowExecutionSignaled(event);
            break;
        case ScheduleActivityTaskFailed:
            activityClient.handleScheduleActivityTaskFailed(event);
            break;
        case SignalExternalWorkflowExecutionFailed:
            workflowClient.handleSignalExternalWorkflowExecutionFailed(event);
            break;
        case StartChildWorkflowExecutionFailed:
            workflowClient.handleStartChildWorkflowExecutionFailed(event);
            break;
        case StartTimerFailed:
            handleStartTimerFailed(event);
            break;
        case TimerFired:
            handleTimerFired(event);
            break;
        case WorkflowExecutionCancelRequested:
            handleWorkflowExecutionCancelRequested(event);
            break;
        case WorkflowExecutionSignaled:
            handleWorkflowExecutionSignaled(event);
            break;
        case WorkflowExecutionStarted:
            handleWorkflowExecutionStarted(event);
            break;
        case WorkflowExecutionTerminated:
            // NOOP
            break;
        case WorkflowExecutionTimedOut:
            // NOOP
            break;
        case ActivityTaskScheduled:
            decisionsHelper.handleActivityTaskScheduled(event);
            break;
        case ActivityTaskCancelRequested:
            decisionsHelper.handleActivityTaskCancelRequested(event);
            break;
        case RequestCancelActivityTaskFailed:
            decisionsHelper.handleRequestCancelActivityTaskFailed(event);
            break;
        case MarkerRecorded:
            break;
        case RecordMarkerFailed:
        	break;
        case WorkflowExecutionCompleted:
            break;
        case CompleteWorkflowExecutionFailed:
            unhandledDecision = true;
            decisionsHelper.handleCompleteWorkflowExecutionFailed(event);
            break;
        case WorkflowExecutionFailed:
            break;
        case FailWorkflowExecutionFailed:
            unhandledDecision = true;
            decisionsHelper.handleFailWorkflowExecutionFailed(event);
            break;
        case WorkflowExecutionCanceled:
            break;
        case CancelWorkflowExecutionFailed:
            unhandledDecision = true;
            decisionsHelper.handleCancelWorkflowExecutionFailed(event);
            break;
        case WorkflowExecutionContinuedAsNew:
            break;
        case ContinueAsNewWorkflowExecutionFailed:
            unhandledDecision = true;
            decisionsHelper.handleContinueAsNewWorkflowExecutionFailed(event);
            break;
        case TimerStarted:
            handleTimerStarted(event);
            break;
        case TimerCanceled:
            workflowClock.handleTimerCanceled(event);
            break;
        case LambdaFunctionScheduled:
            decisionsHelper.handleLambdaFunctionScheduled(event);
            break;
        case LambdaFunctionStarted:
            lambdaFunctionClient.handleLambdaFunctionStarted(event.getLambdaFunctionStartedEventAttributes());
            break;
        case LambdaFunctionCompleted:
            lambdaFunctionClient.handleLambdaFunctionCompleted(event);
            break;
        case LambdaFunctionFailed:
            lambdaFunctionClient.handleLambdaFunctionFailed(event);
            break;
        case LambdaFunctionTimedOut:
            lambdaFunctionClient.handleLambdaFunctionTimedOut(event);
            break;
        case StartLambdaFunctionFailed:
            lambdaFunctionClient.handleStartLambdaFunctionFailed(event);
            break;
        case ScheduleLambdaFunctionFailed:
            lambdaFunctionClient.handleScheduleLambdaFunctionFailed(event);
            break;
        case SignalExternalWorkflowExecutionInitiated:
            decisionsHelper.handleSignalExternalWorkflowExecutionInitiated(event);
            break;
        case RequestCancelExternalWorkflowExecutionInitiated:
            decisionsHelper.handleRequestCancelExternalWorkflowExecutionInitiated(event);
            break;
        case RequestCancelExternalWorkflowExecutionFailed:
            decisionsHelper.handleRequestCancelExternalWorkflowExecutionFailed(event);
            break;
        case StartChildWorkflowExecutionInitiated:
            workflowClient.handleChildWorkflowExecutionInitiated(event);
            break;
        case CancelTimerFailed:
            decisionsHelper.handleCancelTimerFailed(event);
        }
    }

    private void eventLoop(HistoryEvent event) throws Throwable {
        if (completed) {
            return;
        }
        try {
            completed = workflowAsyncScope.eventLoop();
        }
        catch (CancellationException e) {
            if (!cancelRequested) {
                failure = e;
            }
            completed = true;
        } catch (Error e) {
            throw e;
        }
        catch (Throwable e) {
            failure = e;
            completed = true;
        }
    }

    private void completeWorkflowIfCompleted() {
        if (completed && !unhandledDecision) {
            if (failure != null) {
                decisionsHelper.failWorkflowExecution(failure);
            }
            else if (cancelRequested) {
                decisionsHelper.cancelWorkflowExecution();
            }
            else {
                ContinueAsNewWorkflowExecutionParameters continueAsNewOnCompletion = workflowContext.getContinueAsNewOnCompletion();
                if (continueAsNewOnCompletion != null) {
                    decisionsHelper.continueAsNewWorkflowExecution(continueAsNewOnCompletion);
                }
                else {
                    Promise<String> output = workflowAsyncScope.getOutput();
                    if (output != null && output.isReady()) {
                        String workflowOutput = output.get();
                        decisionsHelper.completeWorkflowExecution(workflowOutput);
                    }
                    else {
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
        StartTimerFailedEventAttributes attributes = event.getStartTimerFailedEventAttributes();
        String timerId = attributes.getTimerId();
        if (timerId.equals(DecisionsHelper.FORCE_IMMEDIATE_DECISION_TIMER)) {
            return;
        }
        workflowClock.handleStartTimerFailed(event);
    }

    private void handleTimerFired(HistoryEvent event) throws Throwable {
        TimerFiredEventAttributes attributes = event.getTimerFiredEventAttributes();
        String timerId = attributes.getTimerId();
        if (timerId.equals(DecisionsHelper.FORCE_IMMEDIATE_DECISION_TIMER)) {
            return;
        }
        workflowClock.handleTimerFired(event.getEventId(), attributes);
    }

    private void handleTimerStarted(HistoryEvent event) {
        TimerStartedEventAttributes attributes = event.getTimerStartedEventAttributes();
        String timerId = attributes.getTimerId();
        if (timerId.equals(DecisionsHelper.FORCE_IMMEDIATE_DECISION_TIMER)) {
            return;
        }
        decisionsHelper.handleTimerStarted(event);
    }

    private void handleWorkflowExecutionSignaled(HistoryEvent event) throws Throwable {
        assert (event.getEventType().equals(EventType.WorkflowExecutionSignaled.toString()));
        final WorkflowExecutionSignaledEventAttributes signalAttributes = event.getWorkflowExecutionSignaledEventAttributes();
        if (completed) {
            workflowAsyncScope = new UnhandledSignalAsyncScope(workflowAsyncScope.getOutput(), workflowAsyncScope.getFailure(),
                    workflowAsyncScope.isCancelRequested());
            completed = false;
        }
        // This task is attached to the root context of the workflowAsyncScope
        new Task(workflowAsyncScope) {

            @Override
            protected void doExecute() throws Throwable {
                definition.signalRecieved(signalAttributes.getSignalName(), signalAttributes.getInput());
            }

        };
    }

    private void handleDecisionTaskCompleted(HistoryEvent event) {
        decisionsHelper.handleDecisionCompletion(event.getDecisionTaskCompletedEventAttributes());
    }

    public void decide() throws Exception {
        try {
            try {
            definition = workflowDefinitionFactory.getWorkflowDefinition(context);
            } catch (Exception e) {
                throw new Error("Failed to get workflow definition for " + context, e);
            }
            if (definition == null) {
            	throw new IllegalStateException("Null workflow definition returned for: " + context.getWorkflowContext().getWorkflowType());
            }
            decideImpl();
        }
        catch (AmazonServiceException e) {
            // We don't want to fail workflow on service exceptions like 500 or throttling
            // Throwing from here drops decision task which is OK as it is rescheduled after its StartToClose timeout.
            if (e.getErrorType() == ErrorType.Client && !"ThrottlingException".equals(e.getErrorCode())) {
                if (log.isErrorEnabled()) {
                    log.error("Failing workflow " + workflowContext.getWorkflowExecution(), e);
                }
                decisionsHelper.failWorkflowDueToUnexpectedError(e);
            }
            else {
                throw e;
            }
        } catch (Error e) {
            // Do not fail workflow on Error. Fail the decision.
            throw e;
        }
        catch (Throwable e) {
            if (log.isErrorEnabled()) {
                log.error("Failing workflow " + workflowContext.getWorkflowExecution(), e);
            }
            decisionsHelper.failWorkflowDueToUnexpectedError(e);
        }
        finally {
            try {
            	if (definition != null) {
                    decisionsHelper.setWorkflowContextData(definition.getWorkflowState());
                }
            }
            catch (WorkflowException e) {
                decisionsHelper.setWorkflowContextData(e.getDetails());
            }
            catch (Throwable e) {
                decisionsHelper.setWorkflowContextData(e.getMessage());
            }
            if (definition != null) {
                workflowDefinitionFactory.deleteWorkflowDefinition(definition);
            }
        }
    }

    private void decideImpl() throws Exception, Throwable {
        List<HistoryEvent> singleDecisionEvents = historyHelper.getSingleDecisionEvents();
        while(singleDecisionEvents != null && singleDecisionEvents.size() > 0) {
            decisionsHelper.handleDecisionTaskStartedEvent();
            for (HistoryEvent event : singleDecisionEvents) {
                Long lastNonReplayedEventId = historyHelper.getLastNonReplayEventId();
                if (event.getEventId() >= lastNonReplayedEventId) {
                    workflowClock.setReplaying(false);
                }
                long replayCurrentTimeMilliseconds = historyHelper.getReplayCurrentTimeMilliseconds();
                workflowClock.setReplayCurrentTimeMilliseconds(replayCurrentTimeMilliseconds);
                EventType eventType = EventType.valueOf(event.getEventType());
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

    public DecisionsHelper getDecisionsHelper() {
        return decisionsHelper;
    }

    public WorkflowDefinition getWorkflowDefinition() {
        return definition;
    }

}
