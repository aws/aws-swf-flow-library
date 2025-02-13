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
package com.amazonaws.services.simpleworkflow.flow;

import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementation;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import java.util.concurrent.CancellationException;
import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.swf.SwfClient;

/**
 * Context object passed to an activity implementation.
 * 
 * @see ActivityImplementation
 * 
 * @author fateev
 */
public abstract class ActivityExecutionContext {

    @Getter
    private final ActivityTask task;

    protected ActivityExecutionContext(ActivityTask task) {
        this.task = task;
    }

    /**
     * @return workfow execution that requested the activity execution
     */
    public WorkflowExecution getWorkflowExecution() {
        return task.getWorkflowExecution();
    }

    /**
     * Use to notify Simple Workflow that activity execution is alive.
     *
     * @param details
     *            In case of activity timeout details are returned as a field of
     *            the exception thrown.
     * @throws SdkException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AwsServiceException
     *             If an error response is returned by AmazonSimpleWorkflow
     *             indicating either a problem with the data in the request.
     *             Internal service errors are swallowed and not propagated to
     *             the caller.
     * @throws CancellationException
     *             Indicates that activity cancellation was requested by the
     *             workflow.Should be rethrown from activity implementation to
     *             indicate successful cancellation.
     */
    public abstract void recordActivityHeartbeat(String details)
            throws AwsServiceException, SdkException, CancellationException;

    /**
     * @return an instance of the Simple Workflow Java client that is the same
     *         used by the invoked activity worker.
     */
    public abstract SwfClient getService();

    public String getDomain() {
        // Throwing implementation is provided to not break existing subclasses
        throw new UnsupportedOperationException();
    }
}
