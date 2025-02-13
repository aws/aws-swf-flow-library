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

import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import software.amazon.awssdk.services.swf.model.Decision;
import software.amazon.awssdk.services.swf.model.DecisionType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.RequestCancelExternalWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.StartChildWorkflowExecutionDecisionAttributes;

class ChildWorkflowDecisionStateMachine extends DecisionStateMachineBase {

    private final StartChildWorkflowExecutionDecisionAttributes startAttributes;

    private String workflowId;

    private SimpleWorkflowClientConfig clientConfig;

    private BackoffThrottlerWithJitter throttler;

    public ChildWorkflowDecisionStateMachine(DecisionId id, StartChildWorkflowExecutionDecisionAttributes startAttributes, SimpleWorkflowClientConfig clientConfig) {
        super(id);
        this.startAttributes = startAttributes;
        this.workflowId = startAttributes.workflowId();
        this.clientConfig = clientConfig;
        throttler = new BackoffThrottlerWithJitter(clientConfig.getInitialSleepForDecisionThrottleInMillis(), clientConfig.getMaxSleepForDecisionThrottleInMillis(), clientConfig.getBackoffCoefficientForDecisionThrottle());
    }

    /**
     * Used for unit testing
     */
    ChildWorkflowDecisionStateMachine(DecisionId id, StartChildWorkflowExecutionDecisionAttributes startAttributes, DecisionState state, SimpleWorkflowClientConfig clientConfig) {
        super(id, state);
        this.startAttributes = startAttributes;
        this.workflowId = startAttributes.workflowId();
        this.clientConfig = clientConfig;
        throttler = new BackoffThrottlerWithJitter(clientConfig.getInitialSleepForDecisionThrottleInMillis(), clientConfig.getMaxSleepForDecisionThrottleInMillis(), clientConfig.getBackoffCoefficientForDecisionThrottle());
    }

    @Override
    public Decision getDecision() {
        switch (state) {
        case CREATED:
            return createStartChildWorkflowExecutionDecision();
        case CANCELED_AFTER_STARTED:
            return createRequestCancelExternalWorkflowExecutionDecision();
        default:
            return null;
        }
    }

    @Override
    public void throttleDecision() {
        try {
            throttler.throttle();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleDecisionTaskStartedEvent() {
        switch (state) {
        case CANCELED_AFTER_STARTED:
            state = DecisionState.CANCELLATION_DECISION_SENT;
            break;
        default:
            super.handleDecisionTaskStartedEvent();
        }
    }

    @Override
    public void handleInitiatedEvent(HistoryEvent event) {
        String actualWorkflowId = event.startChildWorkflowExecutionInitiatedEventAttributes().workflowId();
        if (!workflowId.equals(actualWorkflowId)) {
            workflowId = actualWorkflowId;
            id = new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, actualWorkflowId);
        }
        super.handleInitiatedEvent(event);
    }

    @Override
    public void handleInitiationFailedEvent(HistoryEvent event) {
        String actualWorkflowId = event.startChildWorkflowExecutionFailedEventAttributes().workflowId();
        if (!workflowId.equals(actualWorkflowId)) {
            workflowId = actualWorkflowId;
            id = new DecisionId(DecisionTarget.EXTERNAL_WORKFLOW, actualWorkflowId);
        }
        super.handleInitiationFailedEvent(event);
    }

    @Override
    public void handleStartedEvent(HistoryEvent event) {
        stateHistory.add("handleStartedEvent");
        switch (state) {
        case INITIATED:
            state = DecisionState.STARTED;
            break;
        case CANCELED_AFTER_INITIATED:
            state = DecisionState.CANCELED_AFTER_STARTED;
            break;
        }
        stateHistory.add(state.toString());
    }

    @Override
    public void handleCancellationFailureEvent(HistoryEvent event) {
        switch (state) {
        case CANCELLATION_DECISION_SENT:
            stateHistory.add("handleCancellationFailureEvent");
            if (throttler.getFailureCount() < clientConfig.getMaxRetryForCancelingExternalWorkflow()) {
                throttler.failure();
                state = DecisionState.CANCELED_AFTER_STARTED;
            } else {
                state = DecisionState.STARTED;
            }
            stateHistory.add(state.toString());
            break;
        default:
            super.handleCancellationFailureEvent(event);
        }
    }
    
    @Override
    public void cancel(Runnable immediateCancellationCallback) {
        switch (state) {
        case STARTED:
            stateHistory.add("cancel");
            state = DecisionState.CANCELED_AFTER_STARTED;
            stateHistory.add(state.toString());
            break;
        default:
            super.cancel(immediateCancellationCallback);
        }        
    }

    @Override
    public void handleCancellationEvent() {
        switch (state) {
        case STARTED:
            stateHistory.add("handleCancellationEvent");
            state = DecisionState.COMPLETED;
            stateHistory.add(state.toString());
            break;
        default:
            super.handleCancellationEvent();
        }
    }

    @Override
    public void handleCompletionEvent() {
        switch (state) {
        case STARTED:
        case COMPLETED:
        case CANCELED_AFTER_STARTED:
            stateHistory.add("handleCompletionEvent");
            state = DecisionState.COMPLETED;
            stateHistory.add(state.toString());
            break;
        default:
            super.handleCompletionEvent();
        }
    }

    private Decision createRequestCancelExternalWorkflowExecutionDecision() {
        RequestCancelExternalWorkflowExecutionDecisionAttributes tryCancel = RequestCancelExternalWorkflowExecutionDecisionAttributes.builder()
            .workflowId(workflowId).build();
        return Decision.builder().requestCancelExternalWorkflowExecutionDecisionAttributes(tryCancel)
            .decisionType(DecisionType.REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION).build();
    }

    private Decision createStartChildWorkflowExecutionDecision() {
        return Decision.builder().startChildWorkflowExecutionDecisionAttributes(startAttributes)
            .decisionType(DecisionType.START_CHILD_WORKFLOW_EXECUTION).build();
    }

}
