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

import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution.fromSdkType;
import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowType.fromSdkType;

import com.amazonaws.services.simpleworkflow.flow.ChildWorkflowFailedException;
import com.amazonaws.services.simpleworkflow.flow.ChildWorkflowTerminatedException;
import com.amazonaws.services.simpleworkflow.flow.ChildWorkflowTimedOutException;
import com.amazonaws.services.simpleworkflow.flow.SignalExternalWorkflowException;
import com.amazonaws.services.simpleworkflow.flow.StartChildWorkflowFailedException;
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTask;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTaskCancellationHandler;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTaskCompletionHandle;
import com.amazonaws.services.simpleworkflow.flow.core.Functor;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.generic.SignalExternalWorkflowParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.StartChildWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.StartChildWorkflowReply;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import software.amazon.awssdk.services.swf.model.ChildPolicy;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionCanceledEventAttributes;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionTerminatedEventAttributes;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionTimedOutEventAttributes;
import software.amazon.awssdk.services.swf.model.ExternalWorkflowExecutionSignaledEventAttributes;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.RequestCancelExternalWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.SignalExternalWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.SignalExternalWorkflowExecutionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.StartChildWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.StartChildWorkflowExecutionFailedCause;
import software.amazon.awssdk.services.swf.model.StartChildWorkflowExecutionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.TaskList;

class GenericWorkflowClientImpl implements GenericWorkflowClient {

    private static class StartChildWorkflowReplyImpl implements StartChildWorkflowReply {

        private String workflowId;

        private String runId;

        private final Settable<String> result = new Settable<String>();

        public StartChildWorkflowReplyImpl(String workflowId, String runId, String description) {
            this.runId = runId;
            this.workflowId = workflowId;
            result.setDescription(description);
        }

        @Override
        public String getWorkflowId() {
            return workflowId;
        }

        @Override
        public String getRunId() {
            return runId;
        }

        @Override
        public Promise<String> getResult() {
            return result;
        }

        public void setResult(String value) {
            result.set(value);
        }

    }

    private final class ChildWorkflowCancellationHandler implements ExternalTaskCancellationHandler {

        private final String requestedWorkflowId;

        private final ExternalTaskCompletionHandle handle;

        private ChildWorkflowCancellationHandler(String requestedWorkflowId, ExternalTaskCompletionHandle handle) {
            this.requestedWorkflowId = requestedWorkflowId;
            this.handle = handle;
        }

        @Override
        public void handleCancellation(Throwable cause) {
            String actualWorkflowId = decisions.getActualChildWorkflowId(requestedWorkflowId);
            RequestCancelExternalWorkflowExecutionDecisionAttributes cancelAttributes =
                RequestCancelExternalWorkflowExecutionDecisionAttributes.builder().workflowId(actualWorkflowId).build();

            decisions.requestCancelExternalWorkflowExecution(cancelAttributes, new Runnable() {

                @Override
                public void run() {
                    OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = scheduledExternalWorkflows.remove(actualWorkflowId);
                    if (scheduled == null) {
                        throw new IllegalArgumentException("Workflow \"" + actualWorkflowId + "\" wasn't scheduled");
                    }
                    handle.complete();
                }
            });
        }
    }

    private final DecisionsHelper decisions;

    private final WorkflowContext workflowContext;

    private final Map<String, OpenRequestInfo<StartChildWorkflowReply, WorkflowType>> scheduledExternalWorkflows = new HashMap<String, OpenRequestInfo<StartChildWorkflowReply, WorkflowType>>();

    private final Map<String, OpenRequestInfo<Void, Void>> scheduledSignals = new HashMap<String, OpenRequestInfo<Void, Void>>();

    GenericWorkflowClientImpl(DecisionsHelper decisions, WorkflowContext workflowContext) {
        this.decisions = decisions;
        this.workflowContext = workflowContext;
    }

    @Override
    public Promise<StartChildWorkflowReply> startChildWorkflow(final StartChildWorkflowExecutionParameters parameters) {
        final OpenRequestInfo<StartChildWorkflowReply, WorkflowType> context = new OpenRequestInfo<StartChildWorkflowReply, WorkflowType>();

        String workflowId = parameters.getWorkflowId();
        if (workflowId == null) {
            workflowId = generateUniqueId();
        }
        StartChildWorkflowExecutionDecisionAttributes.Builder attributesBuilder = StartChildWorkflowExecutionDecisionAttributes.builder()
            .workflowType(parameters.getWorkflowType().toSdkType())
            .workflowId(workflowId)
            .input(parameters.getInput())
            .executionStartToCloseTimeout(FlowHelpers.secondsToDuration(parameters.getExecutionStartToCloseTimeoutSeconds()))
            .taskStartToCloseTimeout(FlowHelpers.secondsToDuration(parameters.getTaskStartToCloseTimeoutSeconds()))
            .taskPriority(FlowHelpers.taskPriorityToString(parameters.getTaskPriority()))
            .lambdaRole(parameters.getLambdaRole());

        List<String> tagList = parameters.getTagList();
        if (tagList != null) {
            attributesBuilder.tagList(tagList);
        }
        ChildPolicy childPolicy = parameters.getChildPolicy();
        if (childPolicy != null) {
            attributesBuilder.childPolicy(childPolicy);
        }
        String taskList = parameters.getTaskList();
        if (taskList != null && !taskList.isEmpty()) {
            attributesBuilder.taskList(TaskList.builder().name(taskList).build());
        }
        final StartChildWorkflowExecutionDecisionAttributes attributes = attributesBuilder.build();
        String taskName = "workflowId=" + workflowId + ", workflowType=" + attributes.workflowId();
        new ExternalTask() {

            @Override
            protected ExternalTaskCancellationHandler doExecute(final ExternalTaskCompletionHandle handle) throws Throwable {
                context.setCompletionHandle(handle);
                String workflowId = attributes.workflowId();

                if (scheduledExternalWorkflows.containsKey(decisions.getActualChildWorkflowId(workflowId))) {
                    WorkflowExecution workflowExecution = WorkflowExecution.builder().workflowId(workflowId).build();
                    WorkflowType workflowType = fromSdkType(attributes.workflowType());
                    long fakeEventId = -1;

                    handle.fail(new StartChildWorkflowFailedException(fakeEventId, workflowExecution, workflowType, StartChildWorkflowExecutionFailedCause.WORKFLOW_ALREADY_RUNNING.toString()));
                    return new ChildWorkflowCancellationHandler(workflowId, handle);
            	}

                // should not update this map if the same work flow is running
                decisions.startChildWorkflowExecution(attributes);
                scheduledExternalWorkflows.put(workflowId, context);
                return new ChildWorkflowCancellationHandler(workflowId, handle);
            }
        }.setName(taskName);
        context.setResultDescription("startChildWorkflow " + taskName);
        return context.getResult();
    }

    @Override
    public Promise<String> startChildWorkflow(String workflow, String version, String input) {
        StartChildWorkflowExecutionParameters parameters = new StartChildWorkflowExecutionParameters();
        parameters.setWorkflowType(WorkflowType.builder().name(workflow).version(version).build());
        parameters.setInput(input);
        final Promise<StartChildWorkflowReply> started = startChildWorkflow(parameters);
        return new Functor<String>(started) {

            @Override
            protected Promise<String> doExecute() throws Throwable {
                return started.get().getResult();
            }
        };
    }

    @Override
    public Promise<String> startChildWorkflow(final String workflow, final String version, final Promise<String> input) {
        final Settable<String> result = new Settable<String>();

        new Task(input) {

            @Override
            protected void doExecute() throws Throwable {
                result.chain(startChildWorkflow(workflow, version, input.get()));
            }
        };
        return result;
    }

    @Override
    public Promise<Void> signalWorkflowExecution(final SignalExternalWorkflowParameters parameters) {
        final OpenRequestInfo<Void, Void> context = new OpenRequestInfo<Void, Void>();
        String signalId = decisions.getNextId();
        final SignalExternalWorkflowExecutionDecisionAttributes attributes = SignalExternalWorkflowExecutionDecisionAttributes.builder()
            .control(signalId)
            .signalName(parameters.getSignalName())
            .input(parameters.getInput())
            .runId(parameters.getRunId())
            .workflowId(parameters.getWorkflowId()).build();
        String taskName = "signalId=" + signalId + ", workflowId=" + parameters.getWorkflowId() + ", workflowRunId="
                + parameters.getRunId();
        new ExternalTask() {

            @Override
            protected ExternalTaskCancellationHandler doExecute(final ExternalTaskCompletionHandle handle) throws Throwable {

                decisions.signalExternalWorkflowExecution(attributes);
                context.setCompletionHandle(handle);
                final String finalSignalId = attributes.control();
                scheduledSignals.put(finalSignalId, context);
                return new ExternalTaskCancellationHandler() {

                    @Override
                    public void handleCancellation(Throwable cause) {
                        decisions.cancelSignalExternalWorkflowExecution(finalSignalId, null);
                        OpenRequestInfo<Void, Void> scheduled = scheduledSignals.remove(finalSignalId);
                        if (scheduled == null) {
                            throw new IllegalArgumentException("Signal \"" + finalSignalId + "\" wasn't scheduled");
                        }
                        handle.complete();
                    }
                };
            }
        }.setName(taskName);
        context.setResultDescription("signalWorkflowExecution " + taskName);
        return context.getResult();
    }

    @Override
    public void requestCancelWorkflowExecution(WorkflowExecution execution) {
        RequestCancelExternalWorkflowExecutionDecisionAttributes attributes = RequestCancelExternalWorkflowExecutionDecisionAttributes.builder()
            .workflowId(decisions.getActualChildWorkflowId(execution.getWorkflowId())).runId(execution.getRunId()).build();
        // TODO: See if immediate cancellation needed
        decisions.requestCancelExternalWorkflowExecution(attributes, null);
    }

    @Override
    public void continueAsNewOnCompletion(ContinueAsNewWorkflowExecutionParameters continueParameters) {
        // TODO: add validation to check if continueAsNew is not set 
        workflowContext.setContinueAsNewOnCompletion(continueParameters);
    }

    @Override
    public String generateUniqueId() {
        return decisions.getChildWorkflowIdHandler().generateWorkflowId(workflowContext.getWorkflowExecution(), decisions::getNextId);
    }

    public void handleChildWorkflowExecutionCancelRequested(HistoryEvent event) {
        decisions.handleChildWorkflowExecutionCancelRequested(event);
    }

    void handleChildWorkflowExecutionCanceled(HistoryEvent event) {
        ChildWorkflowExecutionCanceledEventAttributes attributes = event.childWorkflowExecutionCanceledEventAttributes();
        WorkflowExecution execution = fromSdkType(attributes.workflowExecution());
        String workflowId = execution.getWorkflowId();
        if (decisions.handleChildWorkflowExecutionCanceled(workflowId)) {
            OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = scheduledExternalWorkflows.remove(workflowId);
            if (scheduled != null) {
                CancellationException e = new CancellationException();
                ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
                // It is OK to fail with subclass of CancellationException when cancellation requested.
                // It allows passing information about cancellation (details in this case) to the surrounding doCatch block
                completionHandle.fail(e);
            }
        }
    }

    void handleChildWorkflowExecutionInitiated(HistoryEvent event) {
        String actualWorkflowId = event.startChildWorkflowExecutionInitiatedEventAttributes().workflowId();
        String requestedWorkflowId = decisions.getChildWorkflowIdHandler().extractRequestedWorkflowId(actualWorkflowId);
        if (!actualWorkflowId.equals(requestedWorkflowId)) {
            OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = scheduledExternalWorkflows.remove(requestedWorkflowId);
            scheduledExternalWorkflows.put(actualWorkflowId, scheduled);
        }
        decisions.handleStartChildWorkflowExecutionInitiated(event);
    }

    void handleChildWorkflowExecutionStarted(HistoryEvent event) {
        ChildWorkflowExecutionStartedEventAttributes attributes = event.childWorkflowExecutionStartedEventAttributes();
        String workflowId = attributes.workflowExecution().workflowId();
        decisions.handleChildWorkflowExecutionStarted(event);
        OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = scheduledExternalWorkflows.get(workflowId);
        if (scheduled != null) {
            String runId = attributes.workflowExecution().runId();
            Settable<StartChildWorkflowReply> result = scheduled.getResult();
            if (!result.isReady()) {
                String description = "startChildWorkflow workflowId=" + workflowId + ", runId=" + runId;
                result.set(new StartChildWorkflowReplyImpl(workflowId, runId, description));
            }
        }
    }

    void handleChildWorkflowExecutionTimedOut(HistoryEvent event) {
        ChildWorkflowExecutionTimedOutEventAttributes attributes = event.childWorkflowExecutionTimedOutEventAttributes();
        WorkflowExecution execution = fromSdkType(attributes.workflowExecution());
        String workflowId = attributes.workflowExecution().workflowId();
        if (decisions.handleChildWorkflowExecutionClosed(workflowId)) {
            OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = scheduledExternalWorkflows.remove(workflowId);
            if (scheduled != null) {
                Exception failure = new ChildWorkflowTimedOutException(event.eventId(), execution,
                    fromSdkType(attributes.workflowType()));
                ExternalTaskCompletionHandle context = scheduled.getCompletionHandle();
                context.fail(failure);
            }
        }
    }

    void handleChildWorkflowExecutionTerminated(HistoryEvent event) {
        ChildWorkflowExecutionTerminatedEventAttributes attributes = event.childWorkflowExecutionTerminatedEventAttributes();
        WorkflowExecution execution = fromSdkType(attributes.workflowExecution());
        String workflowId = attributes.workflowExecution().workflowId();
        if (decisions.handleChildWorkflowExecutionClosed(workflowId)) {
            OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = scheduledExternalWorkflows.remove(workflowId);
            if (scheduled != null) {
                Exception failure = new ChildWorkflowTerminatedException(event.eventId(), execution,
                    fromSdkType(attributes.workflowType()));
                ExternalTaskCompletionHandle context = scheduled.getCompletionHandle();
                context.fail(failure);
            }
        }
    }

    void handleStartChildWorkflowExecutionFailed(HistoryEvent event) {
        if (decisions.handleStartChildWorkflowExecutionFailed(event)) {
            StartChildWorkflowExecutionFailedEventAttributes attributes = event.startChildWorkflowExecutionFailedEventAttributes();
            String actualWorkflowId = attributes.workflowId();
            String requestedWorkflowId = decisions.getChildWorkflowIdHandler().extractRequestedWorkflowId(actualWorkflowId);
            String workflowId = null;
            OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = null;
            if (scheduledExternalWorkflows.containsKey(requestedWorkflowId)) {
                scheduled = scheduledExternalWorkflows.remove(requestedWorkflowId);
                workflowId = requestedWorkflowId;
            } else if (scheduledExternalWorkflows.containsKey(actualWorkflowId)) {
                scheduled = scheduledExternalWorkflows.remove(actualWorkflowId);
                workflowId = actualWorkflowId;
            }

            if (scheduled != null) {
                WorkflowExecution workflowExecution = WorkflowExecution.builder().workflowId(workflowId).build();
                WorkflowType workflowType = fromSdkType(attributes.workflowType());
                String cause = attributes.causeAsString();
                Exception failure = new StartChildWorkflowFailedException(event.eventId(), workflowExecution, workflowType,
                        cause);
                ExternalTaskCompletionHandle context = scheduled.getCompletionHandle();
                context.fail(failure);
            }
        }
    }

    void handleChildWorkflowExecutionFailed(HistoryEvent event) {
        ChildWorkflowExecutionFailedEventAttributes attributes = event.childWorkflowExecutionFailedEventAttributes();
        WorkflowExecution execution = fromSdkType(attributes.workflowExecution());
        String workflowId = execution.getWorkflowId();
        if (decisions.handleChildWorkflowExecutionClosed(workflowId)) {
            OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = scheduledExternalWorkflows.remove(workflowId);
            if (scheduled != null) {
                String reason = attributes.reason();
                String details = attributes.details();
                Exception failure = new ChildWorkflowFailedException(event.eventId(), execution,
                    fromSdkType(attributes.workflowType()), reason, details);
                ExternalTaskCompletionHandle context = scheduled.getCompletionHandle();
                context.fail(failure);
            }
        }
    }

    void handleChildWorkflowExecutionCompleted(HistoryEvent event) {
        ChildWorkflowExecutionCompletedEventAttributes attributes = event.childWorkflowExecutionCompletedEventAttributes();
        String workflowId = attributes.workflowExecution().workflowId();
        if (decisions.handleChildWorkflowExecutionClosed(workflowId)) {
            OpenRequestInfo<StartChildWorkflowReply, WorkflowType> scheduled = scheduledExternalWorkflows.remove(workflowId);
            if (scheduled != null) {
                ExternalTaskCompletionHandle context = scheduled.getCompletionHandle();
                String result = attributes.result();
                StartChildWorkflowReplyImpl startedReply = (StartChildWorkflowReplyImpl) scheduled.getResult().get();
                startedReply.setResult(result);
                context.complete();
            }
        }
    }

    void handleSignalExternalWorkflowExecutionFailed(HistoryEvent event) {
        SignalExternalWorkflowExecutionFailedEventAttributes attributes = event.signalExternalWorkflowExecutionFailedEventAttributes();
        String signalId = attributes.control();
        if (decisions.handleSignalExternalWorkflowExecutionFailed(signalId)) {
            OpenRequestInfo<Void, Void> signalContextAndResult = scheduledSignals.remove(signalId);
            if (signalContextAndResult != null) {
                WorkflowExecution signaledExecution = WorkflowExecution.builder().workflowId(attributes.workflowId()).runId(attributes.runId()).build();
                Throwable failure = new SignalExternalWorkflowException(event.eventId(), signaledExecution,
                        attributes.causeAsString());
                signalContextAndResult.getCompletionHandle().fail(failure);
            }
        }
    }

    void handleExternalWorkflowExecutionSignaled(HistoryEvent event) {
        ExternalWorkflowExecutionSignaledEventAttributes attributes = event.externalWorkflowExecutionSignaledEventAttributes();
        String signalId = decisions.getSignalIdFromExternalWorkflowExecutionSignaled(attributes.initiatedEventId());
        if (decisions.handleExternalWorkflowExecutionSignaled(signalId)) {
            OpenRequestInfo<Void, Void> signalContextAndResult = scheduledSignals.remove(signalId);
            if (signalContextAndResult != null) {
                signalContextAndResult.getResult().set(null);
                signalContextAndResult.getCompletionHandle().complete();
            }
        }
    }

}
