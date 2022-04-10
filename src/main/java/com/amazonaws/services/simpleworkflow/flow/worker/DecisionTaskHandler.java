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

import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.simpleworkflow.flow.core.AsyncTaskInfo;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.RespondDecisionTaskCompletedRequest;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 * 
 * Base class for workflow task handlers. 
 * 
 * @author fateev, suskin
 * 
 */
public abstract class DecisionTaskHandler {

    /**
     * The implementation should be called when a polling SWF Decider receives a
     * new WorkflowTask.
     *
     * @param decisionTaskIterator
     *            The decision task to handle. The reason for more then one task
     *            being received is pagination of the history. All tasks in the
     *            iterator contain the same information but different pages of
     *            the history. The tasks are loaded lazily when
     *            {@link Iterator#next()} is called. It is expected that the
     *            method implementation aborts decision by rethrowing any
     *            exception from {@link Iterator#next()}.
     */
    public abstract RespondDecisionTaskCompletedRequest handleDecisionTask(Iterator<DecisionTask> decisionTaskIterator) throws Exception;

    public abstract List<AsyncTaskInfo> getAsynchronousThreadDump(Iterator<DecisionTask> decisionTaskIterator) throws Exception;

    public abstract String getAsynchronousThreadDumpAsString(Iterator<DecisionTask> decisionTaskIterator) throws Exception;

    public abstract Object loadWorkflowThroughReplay(Iterator<DecisionTask> decisionTaskIterator) throws Exception;

}
