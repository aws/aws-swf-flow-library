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
package com.amazonaws.services.simpleworkflow.flow.generic;

import com.amazonaws.services.simpleworkflow.flow.ActivityExecutionContext;
import com.amazonaws.services.simpleworkflow.flow.ActivityFailureException;
import com.amazonaws.services.simpleworkflow.flow.ActivityWorker;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeExecutionOptions;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeRegistrationOptions;
import java.util.concurrent.CancellationException;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.RegisterActivityTypeRequest;

/**
 * Base class for activity implementation. Extending
 * {@link ActivityImplementationBase} instead of {@link ActivityImplementation}
 * is recommended.
 * 
 * @see ActivityWorker
 * @see ActivityImplementationBase
 * 
 * @author fateev, suskin
 */
public abstract class ActivityImplementation {

    /**
     * Options passed to the
     * {@link SwfClient#registerActivityType(RegisterActivityTypeRequest)}
     * call.
     * 
     * @return null if activity registration is not required on the worker
     *         startup
     */
    public abstract ActivityTypeRegistrationOptions getRegistrationOptions();

    public abstract ActivityTypeExecutionOptions getExecutionOptions();

    /**
     * Execute external activity or initiate its execution.
     * 
     * @param context
     *            information about activity to be executed. Use
     *            {@link ActivityExecutionContext#getTask()}{@code #input()} to get activity input
     *            arguments.
     * @return result of activity execution.
     */
    public abstract String execute(ActivityExecutionContext context) throws ActivityFailureException, CancellationException;

}
