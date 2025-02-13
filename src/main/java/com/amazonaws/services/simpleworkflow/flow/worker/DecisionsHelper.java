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
import com.amazonaws.services.simpleworkflow.flow.WorkflowException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.swf.model.ActivityTaskCancelRequestedEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskCanceledEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskScheduledEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskTimedOutEventAttributes;
import software.amazon.awssdk.services.swf.model.CancelTimerFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.CancelWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ChildPolicy;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.CompleteWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ContinueAsNewWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.Decision;
import software.amazon.awssdk.services.swf.model.DecisionTaskCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.DecisionType;
import software.amazon.awssdk.services.swf.model.FailWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.LambdaFunctionCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.LambdaFunctionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.LambdaFunctionScheduledEventAttributes;
import software.amazon.awssdk.services.swf.model.LambdaFunctionTimedOutEventAttributes;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.RequestCancelActivityTaskFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.RequestCancelExternalWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.RequestCancelExternalWorkflowExecutionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.ScheduleLambdaFunctionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ScheduleLambdaFunctionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.SignalExternalWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.SignalExternalWorkflowExecutionInitiatedEventAttributes;
import software.amazon.awssdk.services.swf.model.StartChildWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.StartChildWorkflowExecutionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.StartChildWorkflowExecutionInitiatedEventAttributes;
import software.amazon.awssdk.services.swf.model.StartLambdaFunctionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.StartTimerDecisionAttributes;
import software.amazon.awssdk.services.swf.model.StartTimerFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.TimerCanceledEventAttributes;
import software.amazon.awssdk.services.swf.model.TimerStartedEventAttributes;

class DecisionsHelper {
    static final int MAXIMUM_DECISIONS_PER_COMPLETION = 100;

    static final String FORCE_IMMEDIATE_DECISION_TIMER = "FORCE_IMMEDIATE_DECISION";

    private final PollForDecisionTaskResponse task;

    private final ChildWorkflowIdHandler childWorkflowIdHandler;

    private long idCounter;

    private final Map<Long, String> activitySchedulingEventIdToActivityId = new HashMap<Long, String>();

    private final Map<Long, String> signalInitiatedEventIdToSignalId = new HashMap<Long, String>();

    private final Map<Long, String> lambdaSchedulingEventIdToLambdaId = new HashMap<Long, String>();

    private final Map<String, String> childWorkflowRequestedToActualWorkflowId = new HashMap<String, String>();

    /**
     * Use access-order to ensure that decisions are emitted in order of their
     * creation
     */
    private final Map<DecisionId, DecisionStateMachine> decisions = new LinkedHashMap<DecisionId, DecisionStateMachine>(100,
            0.75f, true);

    private Throwable workflowFailureCause;

    private String workflowContextData;
    
    private String workflowContextFromLastDecisionCompletion;

    private SimpleWorkflowClientConfig clientConfig;

    DecisionsHelper(PollForDecisionTaskResponse task, ChildWorkflowIdHandler childWorkflowIdHandler, SimpleWorkflowClientConfig clientConfig) {
        this.task = task;
        this.childWorkflowIdHandler = childWorkflowIdHandler;
        this.clientConfig = clientConfig;
    }

    void scheduleLambdaFunction(ScheduleLambdaFunctionDecisionAttributes schedule) {
        DecisionId decisionId = new DecisionId(DecisionTarget.LAMBDA_FUNCTION, schedule.id());
        addDecision(decisionId, new LambdaFunctionDecisionStateMachine(decisionId, schedule));
    }

    /**
     * @return
     * @return true if cancellation already happened as schedule event was found
     *         in the new decisions list
     */
    boolean requestCancelLambdaFunction(String lambdaId, Runnable immediateCancellationCallback) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.LAMBDA_FUNCTION, lambdaId));
        decision.cancel(immediateCancellationCallback);
        return decision.isDone();
    }

    boolean handleLambdaFunctionClosed(String lambdaId) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.LAMBDA_FUNCTION, lambdaId));
        decision.handleCompletionEvent();
        return decision.isDone();
    }

    boolean handleLambdaFunctionScheduled(HistoryEvent event) {
        LambdaFunctionScheduledEventAttributes attributes = event.lambdaFunctionScheduledEventAttributes();
        String functionId = attributes.id();
        lambdaSchedulingEventIdToLambdaId.put(event.eventId(), functionId);
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.LAMBDA_FUNCTION, functionId), event);
        decision.handleInitiatedEvent(event);
        return decision.isDone();
    }

    public boolean handleScheduleLambdaFunctionFailed(HistoryEvent event) {
        ScheduleLambdaFunctionFailedEventAttributes attributes = event.scheduleLambdaFunctionFailedEventAttributes();
        String functionId = attributes.id();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.LAMBDA_FUNCTION, functionId), event);
        decision.handleInitiationFailedEvent(event);
        return decision.isDone();
    }

    public boolean handleStartLambdaFunctionFailed(HistoryEvent event) {
        StartLambdaFunctionFailedEventAttributes attributes = event.startLambdaFunctionFailedEventAttributes();
        String functionId = getFunctionId(attributes);
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.LAMBDA_FUNCTION, functionId), event);
        decision.handleInitiationFailedEvent(event);
        return decision.isDone();
    }

    void scheduleActivityTask(ScheduleActivityTaskDecisionAttributes schedule) {
        DecisionId decisionId = new DecisionId(DecisionTarget.ACTIVITY, schedule.activityId());
        addDecision(decisionId, new ActivityDecisionStateMachine(decisionId, schedule));
    }

    /**
     * @return
     * @return true if cancellation already happened as schedule event was found
     *         in the new decisions list
     */
    boolean requestCancelActivityTask(String activityId, Runnable immediateCancellationCallback) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.ACTIVITY, activityId));
        decision.cancel(immediateCancellationCallback);
        return decision.isDone();
    }

    boolean handleActivityTaskClosed(String activityId) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.ACTIVITY, activityId));
        decision.handleCompletionEvent();
        return decision.isDone();
    }

    boolean handleActivityTaskScheduled(HistoryEvent event) {
        ActivityTaskScheduledEventAttributes attributes = event.activityTaskScheduledEventAttributes();
        String activityId = attributes.activityId();
        activitySchedulingEventIdToActivityId.put(event.eventId(), activityId);
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.ACTIVITY, activityId), event);
        decision.handleInitiatedEvent(event);
        return decision.isDone();
    }

    public boolean handleScheduleActivityTaskFailed(HistoryEvent event) {
        ScheduleActivityTaskFailedEventAttributes attributes = event.scheduleActivityTaskFailedEventAttributes();
        String activityId = attributes.activityId();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.ACTIVITY, activityId), event);
        decision.handleInitiationFailedEvent(event);
        return decision.isDone();
    }

    boolean handleActivityTaskCancelRequested(HistoryEvent event) {
        ActivityTaskCancelRequestedEventAttributes attributes = event.activityTaskCancelRequestedEventAttributes();
        String activityId = attributes.activityId();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.ACTIVITY, activityId), event);
        decision.handleCancellationInitiatedEvent();
        return decision.isDone();
    }

    public boolean handleActivityTaskCanceled(HistoryEvent event) {
        ActivityTaskCanceledEventAttributes attributes = event.activityTaskCanceledEventAttributes();
        String activityId = getActivityId(attributes);
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.ACTIVITY, activityId), event);
        decision.handleCancellationEvent();
        return decision.isDone();
    }

    boolean handleRequestCancelActivityTaskFailed(HistoryEvent event) {
        RequestCancelActivityTaskFailedEventAttributes attributes = event.requestCancelActivityTaskFailedEventAttributes();
        String activityId = attributes.activityId();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.ACTIVITY, activityId), event);
        decision.handleCancellationFailureEvent(event);
        return decision.isDone();
    }

    void startChildWorkflowExecution(StartChildWorkflowExecutionDecisionAttributes schedule) {
        DecisionId decisionId = new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, schedule.workflowId());
        addDecision(decisionId, new ChildWorkflowDecisionStateMachine(decisionId, schedule, clientConfig));
    }

    void handleStartChildWorkflowExecutionInitiated(HistoryEvent event) {
        StartChildWorkflowExecutionInitiatedEventAttributes attributes = event.startChildWorkflowExecutionInitiatedEventAttributes();
        String actualWorkflowId = attributes.workflowId();
        String requestedWorkflowId = childWorkflowIdHandler.extractRequestedWorkflowId(actualWorkflowId);

        DecisionId originalDecisionId = new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, requestedWorkflowId);
        DecisionStateMachine decision = getDecision(originalDecisionId, event);

        if (!actualWorkflowId.equals(requestedWorkflowId)) {
            childWorkflowRequestedToActualWorkflowId.put(requestedWorkflowId, actualWorkflowId);
            decisions.remove(originalDecisionId);
            addDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, actualWorkflowId), decision);
        }

        decision.handleInitiatedEvent(event);
    }

    public boolean handleStartChildWorkflowExecutionFailed(HistoryEvent event) {
        StartChildWorkflowExecutionFailedEventAttributes attributes = event.startChildWorkflowExecutionFailedEventAttributes();
        String actualWorkflowId = attributes.workflowId();
        String requestedWorkflowId = childWorkflowIdHandler.extractRequestedWorkflowId(actualWorkflowId);

        DecisionStateMachine decision;
        if (!actualWorkflowId.equals(requestedWorkflowId)) {
            childWorkflowRequestedToActualWorkflowId.put(requestedWorkflowId, actualWorkflowId);
            DecisionId originalDecisionId = new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, requestedWorkflowId);
            if (decisions.containsKey(originalDecisionId)) {
                // Canceled before initiated
                decision = decisions.remove(originalDecisionId);
                addDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, actualWorkflowId), decision);
            } else {
                decision = getDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, actualWorkflowId), event);
            }
        } else {
            decision = getDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, actualWorkflowId), event);
        }

        decision.handleInitiationFailedEvent(event);
        return decision.isDone();
    }

    public void handleChildWorkflowExecutionStarted(HistoryEvent event) {
        ChildWorkflowExecutionStartedEventAttributes attributes = event.childWorkflowExecutionStartedEventAttributes();
        String workflowId = attributes.workflowExecution().workflowId();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, workflowId), event);
        decision.handleStartedEvent(event);
    }

    boolean handleChildWorkflowExecutionClosed(String workflowId) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, workflowId));
        decision.handleCompletionEvent();
        return decision.isDone();
    }

    public void handleChildWorkflowExecutionCancelRequested(HistoryEvent event) {
    }

    public boolean handleChildWorkflowExecutionCanceled(String workflowId) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, workflowId));
        decision.handleCancellationEvent();
        return decision.isDone();
    }

    /**
     * @return true if cancellation already happened as schedule event was found
     *         in the new decisions list
     */
    boolean requestCancelExternalWorkflowExecution(RequestCancelExternalWorkflowExecutionDecisionAttributes request,
        Runnable immediateCancellationCallback) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, request.workflowId()));
        decision.cancel(immediateCancellationCallback);
        return decision.isDone();
    }

    void handleRequestCancelExternalWorkflowExecutionInitiated(HistoryEvent event) {
        RequestCancelExternalWorkflowExecutionInitiatedEventAttributes attributes
            = event.requestCancelExternalWorkflowExecutionInitiatedEventAttributes();
        String workflowId = attributes.workflowId();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, workflowId), event);
        decision.handleCancellationInitiatedEvent();
    }

    void handleRequestCancelExternalWorkflowExecutionFailed(HistoryEvent event) {
        RequestCancelExternalWorkflowExecutionFailedEventAttributes attributes
            = event.requestCancelExternalWorkflowExecutionFailedEventAttributes();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, attributes.workflowId()), event);
        decision.handleCancellationFailureEvent(event);
    }

    void signalExternalWorkflowExecution(SignalExternalWorkflowExecutionDecisionAttributes signal) {
        DecisionId decisionId = new DecisionId(DecisionTarget.SIGNAL, signal.control());
        addDecision(decisionId, new SignalDecisionStateMachine(decisionId, signal));
    }

    void cancelSignalExternalWorkflowExecution(String signalId, Runnable immediateCancellationCallback) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.SIGNAL, signalId));
        decision.cancel(immediateCancellationCallback);
    }

    void handleSignalExternalWorkflowExecutionInitiated(HistoryEvent event) {
        SignalExternalWorkflowExecutionInitiatedEventAttributes attributes = event.signalExternalWorkflowExecutionInitiatedEventAttributes();
        String signalId = attributes.control();
        signalInitiatedEventIdToSignalId.put(event.eventId(), signalId);
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.SIGNAL, signalId), event);
        decision.handleInitiatedEvent(event);
    }

    public boolean handleSignalExternalWorkflowExecutionFailed(String signalId) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.SIGNAL, signalId));
        decision.handleCompletionEvent();
        return decision.isDone();
    }

    public boolean handleExternalWorkflowExecutionSignaled(String signalId) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.SIGNAL, signalId));
        decision.handleCompletionEvent();
        return decision.isDone();
    }

    void startTimer(StartTimerDecisionAttributes request, Object createTimerUserContext) {
        String timerId = request.timerId();
        DecisionId decisionId = new DecisionId(DecisionTarget.TIMER, timerId);
        addDecision(decisionId, new TimerDecisionStateMachine(decisionId, request));
    }

    boolean cancelTimer(String timerId, Runnable immediateCancellationCallback) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.TIMER, timerId));
        decision.cancel(immediateCancellationCallback);
        return decision.isDone();
    }

    boolean handleTimerClosed(String timerId) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.TIMER, timerId));
        decision.handleCompletionEvent();
        return decision.isDone();
    }

    boolean handleTimerStarted(HistoryEvent event) {
        TimerStartedEventAttributes attributes = event.timerStartedEventAttributes();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.TIMER, attributes.timerId()), event);
        decision.handleInitiatedEvent(event);
        return decision.isDone();
    }

    public boolean handleStartTimerFailed(HistoryEvent event) {
        StartTimerFailedEventAttributes attributes = event.startTimerFailedEventAttributes();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.TIMER, attributes.timerId()), event);
        decision.handleInitiationFailedEvent(event);
        return decision.isDone();
    }

    boolean handleTimerCanceled(HistoryEvent event) {
        TimerCanceledEventAttributes attributes = event.timerCanceledEventAttributes();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.TIMER, attributes.timerId()), event);
        decision.handleCancellationEvent();
        return decision.isDone();
    }

    boolean handleCancelTimerFailed(HistoryEvent event) {
        CancelTimerFailedEventAttributes attributes = event.cancelTimerFailedEventAttributes();
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.TIMER, attributes.timerId()), event);
        decision.handleCancellationFailureEvent(event);
        return decision.isDone();
    }

    void completeWorkflowExecution(String output) {
        Decision.Builder decisionBuilder = Decision.builder();
        decisionBuilder.decisionType(DecisionType.COMPLETE_WORKFLOW_EXECUTION);
        CompleteWorkflowExecutionDecisionAttributes complete = CompleteWorkflowExecutionDecisionAttributes.builder().result(output).build();
        decisionBuilder.completeWorkflowExecutionDecisionAttributes(complete);
        Decision decision = decisionBuilder.build();
        DecisionId decisionId = new DecisionId(DecisionTarget.SELF, null);
        addDecision(decisionId, new CompleteWorkflowStateMachine(decisionId, decision));
    }

    void continueAsNewWorkflowExecution(ContinueAsNewWorkflowExecutionParameters continueParameters) {
        ContinueAsNewWorkflowExecutionDecisionAttributes.Builder attributeBuilder = ContinueAsNewWorkflowExecutionDecisionAttributes.builder();
        attributeBuilder.workflowTypeVersion(continueParameters.getWorkflowTypeVersion());
        ChildPolicy childPolicy = continueParameters.getChildPolicy();
        if (childPolicy != null) {
            attributeBuilder.childPolicy(childPolicy);
        }
        attributeBuilder.input(continueParameters.getInput());
        attributeBuilder.executionStartToCloseTimeout(
            FlowHelpers.secondsToDuration(continueParameters.getExecutionStartToCloseTimeoutSeconds()));
        attributeBuilder.taskStartToCloseTimeout(FlowHelpers.secondsToDuration(continueParameters.getTaskStartToCloseTimeoutSeconds()));
        attributeBuilder.taskPriority(FlowHelpers.taskPriorityToString(continueParameters.getTaskPriority()));
        List<String> tagList = continueParameters.getTagList();
        if (tagList != null) {
            attributeBuilder.tagList(tagList);
        }
        String taskList = continueParameters.getTaskList();
        if (taskList != null && !taskList.isEmpty()) {
            attributeBuilder.taskList(TaskList.builder().name(taskList).build());
        }
        attributeBuilder.lambdaRole(continueParameters.getLambdaRole());

        Decision decision = Decision.builder().decisionType(DecisionType.CONTINUE_AS_NEW_WORKFLOW_EXECUTION)
            .continueAsNewWorkflowExecutionDecisionAttributes(attributeBuilder.build()).build();

        DecisionId decisionId = new DecisionId(DecisionTarget.SELF, null);
        addDecision(decisionId, new CompleteWorkflowStateMachine(decisionId, decision));
    }

    void failWorkflowExecution(Throwable e) {
        FailWorkflowExecutionDecisionAttributes fail = createFailWorkflowInstanceAttributes(e);
        Decision decision = Decision.builder().failWorkflowExecutionDecisionAttributes(fail)
            .decisionType(DecisionType.FAIL_WORKFLOW_EXECUTION).build();
        DecisionId decisionId = new DecisionId(DecisionTarget.SELF, null);
        addDecision(decisionId, new CompleteWorkflowStateMachine(decisionId, decision));
        workflowFailureCause = e;
    }

    void failWorkflowDueToUnexpectedError(Throwable e) {
        // To make sure that failure goes through do not make any other decisions
        decisions.clear();
        this.failWorkflowExecution(e);
        ThreadLocalMetrics.getMetrics().recordCount(MetricName.FAIL_WORKFLOW_UNEXPECTED_ERROR.getName(), 1);
    }

    void handleCompleteWorkflowExecutionFailed(HistoryEvent event) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.SELF, null), event);
        decision.handleInitiationFailedEvent(event);
    }

    void handleFailWorkflowExecutionFailed(HistoryEvent event) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.SELF, null), event);
        decision.handleInitiationFailedEvent(event);
    }

    void handleCancelWorkflowExecutionFailed(HistoryEvent event) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.SELF, null), event);
        decision.handleInitiationFailedEvent(event);
    }

    void handleContinueAsNewWorkflowExecutionFailed(HistoryEvent event) {
        DecisionStateMachine decision = getDecision(new DecisionId(DecisionTarget.SELF, null), event);
        decision.handleInitiationFailedEvent(event);
    }

    /**
     * @return <code>false</code> means that cancel failed, <code>true</code>
     *         that CancelWorkflowExecution was created.
     */
    void cancelWorkflowExecution() {
        CancelWorkflowExecutionDecisionAttributes cancel = CancelWorkflowExecutionDecisionAttributes.builder().details(null).build();
        Decision decision = Decision.builder().cancelWorkflowExecutionDecisionAttributes(cancel)
            .decisionType(DecisionType.CANCEL_WORKFLOW_EXECUTION).build();

        DecisionId decisionId = new DecisionId(DecisionTarget.SELF, null);
        addDecision(decisionId, new CompleteWorkflowStateMachine(decisionId, decision));
    }

    List<Decision> getDecisions() {
        List<Decision> result = new ArrayList<>(MAXIMUM_DECISIONS_PER_COMPLETION + 1);
        for (DecisionStateMachine decisionStateMachine : decisions.values()) {
            Decision decision = decisionStateMachine.getDecision();
            if (decision != null) {
                result.add(decision);
                decisionStateMachine.throttleDecision();
            }
        }
        // Include FORCE_IMMEDIATE_DECISION timer only if there are more than 100 events
        int size = result.size();
        if (size > MAXIMUM_DECISIONS_PER_COMPLETION && !isCompletionEvent(result.get(MAXIMUM_DECISIONS_PER_COMPLETION - 2))) {
            ThreadLocalMetrics.getMetrics().recordCount(MetricName.DECISION_LIST_TRUNCATED.getName(), 1);
            result = result.subList(0, MAXIMUM_DECISIONS_PER_COMPLETION - 1);
            StartTimerDecisionAttributes attributes = StartTimerDecisionAttributes.builder().startToFireTimeout("0")
                .timerId(FORCE_IMMEDIATE_DECISION_TIMER).build();
            Decision d = Decision.builder().startTimerDecisionAttributes(attributes).decisionType(DecisionType.START_TIMER).build();
            result.add(d);
        }

        return result;
    }

    private boolean isCompletionEvent(Decision decision) {
        DecisionType type = DecisionType.fromValue(decision.decisionTypeAsString());
        switch (type) {
        case CANCEL_WORKFLOW_EXECUTION:
        case COMPLETE_WORKFLOW_EXECUTION:
        case FAIL_WORKFLOW_EXECUTION:
        case CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
            return true;
        default:
            return false;
        }
    }

    public void handleDecisionTaskStartedEvent() {
        int count = 0;
        Iterator<DecisionStateMachine> iterator = decisions.values().iterator();
        DecisionStateMachine next = null;

        DecisionStateMachine decisionStateMachine = getNextDecision(iterator);
        while (decisionStateMachine != null) {
            next = getNextDecision(iterator);
            if (++count == MAXIMUM_DECISIONS_PER_COMPLETION && next != null && !isCompletionEvent(next.getDecision())) {
                break;
            }
            decisionStateMachine.handleDecisionTaskStartedEvent();
            decisionStateMachine = next;
        }
        if (next != null && count < MAXIMUM_DECISIONS_PER_COMPLETION) {
            next.handleDecisionTaskStartedEvent();
        }
    }

    private DecisionStateMachine getNextDecision(Iterator<DecisionStateMachine> iterator) {
        DecisionStateMachine result = null;
        while (result == null && iterator.hasNext()) {
            result = iterator.next();
            if (result.getDecision() == null) {
                result = null;
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return WorkflowExecutionUtils.prettyPrintDecisions(getDecisions());
    }

    boolean isWorkflowFailed() {
        return workflowFailureCause != null;
    }

    public Throwable getWorkflowFailureCause() {
        return workflowFailureCause;
    }

    String getWorkflowContextData() {
        return workflowContextData;
    }

    void setWorkflowContextData(String workflowState) {
        this.workflowContextData = workflowState;
    }

    /**
     * @return new workflow state or null if it didn't change since the last
     *         decision completion
     */
    String getWorkflowContextDataToReturn() {
        if (workflowContextFromLastDecisionCompletion == null
                || !workflowContextFromLastDecisionCompletion.equals(workflowContextData)) {
        	return workflowContextData;
        }
        return null;
    }

    void handleDecisionCompletion(DecisionTaskCompletedEventAttributes decisionTaskCompletedEventAttributes) {
        workflowContextFromLastDecisionCompletion = decisionTaskCompletedEventAttributes.executionContext();
    }

    PollForDecisionTaskResponse getTask() {
        return task;
    }

    String getActualChildWorkflowId(String requestedWorkflowId) {
        String result = childWorkflowRequestedToActualWorkflowId.get(requestedWorkflowId);
        return result == null ? requestedWorkflowId : result;
    }

    String getActivityId(ActivityTaskCanceledEventAttributes attributes) {
        Long sourceId = attributes.scheduledEventId();
        return activitySchedulingEventIdToActivityId.get(sourceId);
    }

    String getActivityId(ActivityTaskCompletedEventAttributes attributes) {
        Long sourceId = attributes.scheduledEventId();
        return activitySchedulingEventIdToActivityId.get(sourceId);
    }

    String getActivityId(ActivityTaskFailedEventAttributes attributes) {
        Long sourceId = attributes.scheduledEventId();
        return activitySchedulingEventIdToActivityId.get(sourceId);
    }

    String getActivityId(ActivityTaskTimedOutEventAttributes attributes) {
        Long sourceId = attributes.scheduledEventId();
        return activitySchedulingEventIdToActivityId.get(sourceId);
    }

    String getFunctionId(LambdaFunctionCompletedEventAttributes attributes) {
        Long sourceId = attributes.scheduledEventId();
        return lambdaSchedulingEventIdToLambdaId.get(sourceId);
    }

    String getFunctionId(LambdaFunctionFailedEventAttributes attributes) {
        Long sourceId = attributes.scheduledEventId();
        return lambdaSchedulingEventIdToLambdaId.get(sourceId);
    }

    String getFunctionId(LambdaFunctionTimedOutEventAttributes attributes) {
        Long sourceId = attributes.scheduledEventId();
        return lambdaSchedulingEventIdToLambdaId.get(sourceId);
    }

    String getFunctionId(StartLambdaFunctionFailedEventAttributes attributes) {
        Long sourceId = attributes.scheduledEventId();
        return lambdaSchedulingEventIdToLambdaId.get(sourceId);
    }

    String getSignalIdFromExternalWorkflowExecutionSignaled(long initiatedEventId) {
        return signalInitiatedEventIdToSignalId.get(initiatedEventId);
    }

    private FailWorkflowExecutionDecisionAttributes createFailWorkflowInstanceAttributes(Throwable failure) {
        String reason;
        String details;
        if (failure instanceof WorkflowException) {
            WorkflowException f = (WorkflowException) failure;
            reason = f.getReason();
            details = f.getDetails();
        } else {
            reason = failure.getMessage();
            StringWriter sw = new StringWriter();
            failure.printStackTrace(new PrintWriter(sw));
            details = sw.toString();
        }
        return FailWorkflowExecutionDecisionAttributes.builder().reason(WorkflowExecutionUtils.truncateReason(reason))
            .details(WorkflowExecutionUtils.truncateDetails(details))
            .build();
    }

    void addDecision(DecisionId decisionId, DecisionStateMachine decision) {
        decisions.put(decisionId, decision);
    }

    private DecisionStateMachine getDecision(DecisionId decisionId) {
        return getDecision(decisionId, null);
    }

    private DecisionStateMachine getDecision(DecisionId decisionId, HistoryEvent event) {
        DecisionStateMachine result = decisions.get(decisionId);

        if (result == null) {
            throw new IncompatibleWorkflowDefinition("Unknown " + decisionId + ". The possible causes are "
                + "nondeterministic workflow definition code or incompatible change in the workflow definition. "
                + (event != null ? "HistoryEvent: " + event.toString() + ". " : "")
                + "Keys in the decisions map: " + decisions.keySet());
        }

        return result;
    }

    public ChildWorkflowIdHandler getChildWorkflowIdHandler() {
        return childWorkflowIdHandler;
    }

    public String getNextId() {
        return String.valueOf(++idCounter);
    }

}
