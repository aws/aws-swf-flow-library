/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow.test;

import com.amazonaws.services.simpleworkflow.flow.DecisionContextProvider;
import com.amazonaws.services.simpleworkflow.flow.DecisionContextProviderImpl;
import com.amazonaws.services.simpleworkflow.flow.LambdaFunctionException;
import com.amazonaws.services.simpleworkflow.flow.LambdaFunctionFailedException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.worker.LambdaFunctionClient;

public class TestLambdaFunctionClient implements LambdaFunctionClient {
	protected final DecisionContextProvider decisionContextProvider;

	protected TestLambdaFunctionInvoker invoker;

	public TestLambdaFunctionClient() {
		this.decisionContextProvider = new DecisionContextProviderImpl();
	}

	@Override
	public Promise<String> scheduleLambdaFunction(final String name,
			final String input) {
		return scheduleLambdaFunction(name, input,
				FlowConstants.DEFAULT_LAMBDA_FUNCTION_TIMEOUT);
	}

	@Override
	public Promise<String> scheduleLambdaFunction(final String name,
			final Promise<String> input) {
		return scheduleLambdaFunction(name, input,
				FlowConstants.DEFAULT_LAMBDA_FUNCTION_TIMEOUT);
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
		final String functionId = decisionContextProvider.getDecisionContext()
				.getWorkflowClient().generateUniqueId();
		return scheduleLambdaFunction(name, input, timeoutSeconds, functionId);
	}

	@Override
	public Promise<String> scheduleLambdaFunction(String name, String input,
			long timeoutSeconds, final String functionId) {
		final Settable<String> result = new Settable<String>();
		try {
			result.set(invoker.invoke(name, input, timeoutSeconds));
		} catch (Throwable e) {
			if (e instanceof LambdaFunctionException) {
				throw (LambdaFunctionException) e;
			} else {
				LambdaFunctionFailedException failure = new LambdaFunctionFailedException(
						0, name, functionId, e.getMessage());
				failure.initCause(e);
				throw failure;
			}
		}
		return result;
	}

	public void setInvoker(TestLambdaFunctionInvoker invoker) {
		this.invoker = invoker;
	}
}
