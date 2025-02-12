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

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.simpleworkflow.flow.LambdaFunctionFailedException;
import com.amazonaws.services.simpleworkflow.flow.LambdaFunctionTimedOutException;
import com.amazonaws.services.simpleworkflow.flow.ScheduleLambdaFunctionFailedException;
import com.amazonaws.services.simpleworkflow.flow.StartLambdaFunctionFailedException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTask;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTaskCancellationHandler;
import com.amazonaws.services.simpleworkflow.flow.core.ExternalTaskCompletionHandle;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.LambdaFunctionCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.LambdaFunctionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.LambdaFunctionStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.LambdaFunctionTimedOutEventAttributes;
import software.amazon.awssdk.services.swf.model.ScheduleLambdaFunctionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ScheduleLambdaFunctionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.StartLambdaFunctionFailedEventAttributes;

public class LambdaFunctionClientImpl implements LambdaFunctionClient {
	private final class LambdaFunctionCancellationHandler implements
			ExternalTaskCancellationHandler {

		private final String lambdaId;

		private LambdaFunctionCancellationHandler(String lambdaId) {
			this.lambdaId = lambdaId;
		}

		@Override
		public void handleCancellation(Throwable cause) {
			// do not support cancellation
		}
	}

	private final DecisionsHelper decisions;

	private final Map<String, OpenRequestInfo<String, String>> scheduledLambdas = new HashMap<String, OpenRequestInfo<String, String>>();

	public LambdaFunctionClientImpl(DecisionsHelper decisions) {
		this.decisions = decisions;
	}

	@Override
	public Promise<String> scheduleLambdaFunction(final String name,
			final String input) {
		return scheduleLambdaFunction(name, input, 0);
	}

	@Override
	public Promise<String> scheduleLambdaFunction(final String name,
			final Promise<String> input) {
		return scheduleLambdaFunction(name, input, 0);
	}

	@Override
	public Promise<String> scheduleLambdaFunction(final String name,
			final Promise<String> input, final long timeoutSeconds) {
		final Settable<String> result = new Settable<String>();
		new Task(input) {

			@Override
			protected void doExecute() throws Throwable {
				result.chain(scheduleLambdaFunction(name, input.get(),
						timeoutSeconds));
			}
		};
		return result;
	}

	@Override
	public Promise<String> scheduleLambdaFunction(final String name,
			final String input, final long timeoutSeconds) {
		final String functionId = decisions.getNextId();
		return scheduleLambdaFunction(name, input, timeoutSeconds, functionId);
	}

	@Override
	public Promise<String> scheduleLambdaFunction(final String name,
			final String input, final long timeoutSeconds, final String functionId) {

		if (timeoutSeconds < 0) {
			throw new IllegalArgumentException("Negative timeoutSeconds: "
					+ timeoutSeconds);
		}

		final OpenRequestInfo<String, String> context = new OpenRequestInfo<String, String>(
				name);
		ScheduleLambdaFunctionDecisionAttributes.Builder attributesBuilder
			= ScheduleLambdaFunctionDecisionAttributes.builder()
			.name(name).input(input).id(functionId);
		if (timeoutSeconds == 0) {
			attributesBuilder.startToCloseTimeout(FlowHelpers
				.secondsToDuration(FlowConstants.DEFAULT_LAMBDA_FUNCTION_TIMEOUT));
		} else {
			attributesBuilder.startToCloseTimeout(FlowHelpers
				.secondsToDuration(timeoutSeconds));
		}
		final ScheduleLambdaFunctionDecisionAttributes attributes = attributesBuilder.build();
		String taskName = "functionId=" + attributes.id() + ", timeouts="
				+ attributes.startToCloseTimeout();
		new ExternalTask() {

			@Override
			protected ExternalTaskCancellationHandler doExecute(
					ExternalTaskCompletionHandle handle) throws Throwable {
				decisions.scheduleLambdaFunction(attributes);
				context.setCompletionHandle(handle);
				scheduledLambdas.put(functionId, context);
				return new LambdaFunctionCancellationHandler(functionId);
			}
		}.setName(taskName);
		context.setResultDescription("createTimer " + taskName);
		return context.getResult();
	}

	void handleLambdaFunctionStarted(
			LambdaFunctionStartedEventAttributes attributes) {
	}

	void handleStartLambdaFunctionFailed(HistoryEvent event) {
		StartLambdaFunctionFailedEventAttributes startLambdaFunctionFailedAttributes = event
				.startLambdaFunctionFailedEventAttributes();
		String functionId = decisions.getFunctionId(startLambdaFunctionFailedAttributes);
		OpenRequestInfo<String, String> scheduled = scheduledLambdas.remove(functionId);
		if (decisions.handleStartLambdaFunctionFailed(event)) {
			String cause = startLambdaFunctionFailedAttributes.causeAsString();
			StartLambdaFunctionFailedException failure = new StartLambdaFunctionFailedException(event.eventId(),
			        scheduled.getUserContext(), functionId, cause);
			ExternalTaskCompletionHandle completionHandle = scheduled.getCompletionHandle();
			completionHandle.fail(failure);
		}
	}

	void handleScheduleLambdaFunctionFailed(HistoryEvent event) {
		ScheduleLambdaFunctionFailedEventAttributes attributes = event
				.scheduleLambdaFunctionFailedEventAttributes();
		String functionId = attributes.id();
		OpenRequestInfo<String, String> scheduled = scheduledLambdas
				.remove(functionId);
		if (decisions.handleScheduleLambdaFunctionFailed(event)) {
			String cause = attributes.causeAsString();
			ScheduleLambdaFunctionFailedException failure = new ScheduleLambdaFunctionFailedException(
					event.eventId(), attributes.name(), functionId, cause);
			ExternalTaskCompletionHandle completionHandle = scheduled
					.getCompletionHandle();
			completionHandle.fail(failure);
		}
	} 

	void handleLambdaFunctionCompleted(HistoryEvent event) {
		LambdaFunctionCompletedEventAttributes attributes = event
				.lambdaFunctionCompletedEventAttributes();
		String lambdaId = decisions.getFunctionId(attributes);
		if (decisions.handleLambdaFunctionClosed(lambdaId)) {
			OpenRequestInfo<String, String> scheduled = scheduledLambdas
					.remove(lambdaId);
			if (scheduled != null) {
				String result = attributes.result();
				scheduled.getResult().set(result);
				ExternalTaskCompletionHandle completionHandle = scheduled
						.getCompletionHandle();
				completionHandle.complete();
			}
		}
	}

	void handleLambdaFunctionFailed(HistoryEvent event) {
		LambdaFunctionFailedEventAttributes attributes = event
				.lambdaFunctionFailedEventAttributes();
		String functionId = decisions.getFunctionId(attributes);
		if (decisions.handleLambdaFunctionClosed(functionId)) {
			OpenRequestInfo<String, String> scheduled = scheduledLambdas
					.remove(functionId);
			if (scheduled != null) {
				String detail = attributes.details();
				LambdaFunctionFailedException failure = new LambdaFunctionFailedException(
						event.eventId(), scheduled.getUserContext(),
						functionId, detail);
				ExternalTaskCompletionHandle completionHandle = scheduled
						.getCompletionHandle();
				completionHandle.fail(failure);
			}
		}
	}

	void handleLambdaFunctionTimedOut(HistoryEvent event) {
		LambdaFunctionTimedOutEventAttributes attributes = event
				.lambdaFunctionTimedOutEventAttributes();
		String functionId = decisions.getFunctionId(attributes);
		if (decisions.handleLambdaFunctionClosed(functionId)) {
			OpenRequestInfo<String, String> scheduled = scheduledLambdas
					.remove(functionId);
			if (scheduled != null) {
				String timeoutType = attributes.timeoutTypeAsString();
				LambdaFunctionTimedOutException failure = new LambdaFunctionTimedOutException(
						event.eventId(), scheduled.getUserContext(),
						functionId, timeoutType);
				ExternalTaskCompletionHandle completionHandle = scheduled
						.getCompletionHandle();
				completionHandle.fail(failure);
			}
		}
	}

}
