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
package com.amazonaws.services.simpleworkflow.flow.generic;

import com.amazonaws.services.simpleworkflow.flow.WorkflowException;
import com.amazonaws.services.simpleworkflow.flow.annotations.Asynchronous;
import com.amazonaws.services.simpleworkflow.flow.annotations.Execute;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.core.TryCatchFinally;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.DescribeWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.RespondDecisionTaskCompletedRequest;

/**
 * Base class for all workflow definitions. Implementation should use
 * {@link Execute} to specify workflow name different from implementation class
 * name, different workflow version and other workflow instance registration and
 * execution parameters.
 * 
 * @see Execute
 * @author fateev
 */
public abstract class WorkflowDefinition {

    /**
     * Asynchronous method that implements workflow business logic. This method
     * invocation is surrounded by {@link TryCatchFinally}. Workflow is
     * completed when {@link TryCatchFinally#doFinally()} is executed. So even
     * if return {@link Promise} of the method is ready but there is some
     * asynchronous task or activity still not completed workflow is not going
     * to complete.
     * 
     * @param input
     *            Data passed to the worklfow instance during start instance
     *            call.
     * @return
     * @throws WorkflowException
     *             Prefer throwing {@link WorkflowException}.
     */
    public abstract Promise<String> execute(String input) throws WorkflowException;

    /**
     * Asynchronous method that implements signals handling logic. This method
     * invocation is surrounded by the same doTry of {@link TryCatchFinally}
     * that is used to execute workflow. It means that non handled failure
     * inside this method causes workflow execution failure.
     * 
     * @throws WorkflowException
     *             Prefer throwing {@link WorkflowException}.
     */
    public abstract void signalRecieved(String signalName, String input) throws WorkflowException;

    /**
     * Return state that is inserted decision completion through
     * {@link RespondDecisionTaskCompletedRequest.Builder#executionContext(String)}
     * and later can be retrieved through
     * {@link SwfClient#describeWorkflowExecution(DescribeWorkflowExecutionRequest)}
     * visibility call.
     * 
     * Implementation of this call is expected to be synchronous and is not
     * allowed to invoke any asynchronous operations like creation of new
     * {@link Task} or calling methods marked with {@link Asynchronous}
     * annotation. It is also expected to be read only operation which is not
     * allowed to modify state of workflow in any way.
     * 
     * @return current state of the workflow execution.
     * @throws WorkflowException
     *             Prefer throwing {@link WorkflowException}.
     */
    public abstract String getWorkflowState() throws WorkflowException;

}
