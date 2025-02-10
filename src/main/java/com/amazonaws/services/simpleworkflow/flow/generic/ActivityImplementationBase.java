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

/**
 * Extend this class to implement an activity. There are two types of activity
 * implementation: synchronous and asynchronous. Synchronous ties thread that
 * calls {@link #execute(String, ActivityExecutionContext)} method.
 * 
 * @see ActivityWorker
 * 
 * @author fateev
 */
public abstract class ActivityImplementationBase extends ActivityImplementation {

    /**
     * @see ActivityImplementation#execute(ActivityExecutionContext)
     */
    @Override
    public String execute(ActivityExecutionContext context) 
            throws ActivityFailureException, CancellationException {
        return execute(context.getTask().getInput(), context);
    }

    @Override
    public ActivityTypeExecutionOptions getExecutionOptions() {
        return new ActivityTypeExecutionOptions();
    }

    /**
     * By default do not register
     */
    @Override
    public ActivityTypeRegistrationOptions getRegistrationOptions() {
        return null;
    }

    /**
     * Execute activity.
     * @param input
     *            activity input.
     * @param context
     *            Context object passed to an activity implementation.
     * @return result of activity execution
     */
    protected abstract String execute(String input, ActivityExecutionContext context) 
            throws ActivityFailureException, CancellationException;

}
