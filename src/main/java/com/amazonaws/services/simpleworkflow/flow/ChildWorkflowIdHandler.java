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

import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import java.util.function.Supplier;

public interface ChildWorkflowIdHandler {

    /**
     * Generate a workflow id for a new child workflow.
     *
     * @param currentWorkflow The current (i.e. parent) workflow execution
     * @param nextId Can be called to get a replay-safe id that is unique in the
     *               context of the current workflow.
     *
     * @return A new child workflow id
     */
    String generateWorkflowId(WorkflowExecution currentWorkflow, Supplier<String> nextId);

    /**
     * Extract the child workflow id that was provided when
     * making a decision to start a child workflow.
     *
     * @param childWorkflowId The actual child workflow id
     * @return The original requested child workflow id (may be the same as the actual child workflow id)
     */
    String extractRequestedWorkflowId(String childWorkflowId);
}
