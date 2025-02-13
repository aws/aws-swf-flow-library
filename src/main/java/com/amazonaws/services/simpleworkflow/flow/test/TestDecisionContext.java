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

import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericActivityClient;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.worker.LambdaFunctionClient;

public class TestDecisionContext extends DecisionContext {

    private final GenericActivityClient activityClient;
    private final GenericWorkflowClient workflowClient;
    private final WorkflowClock workflowClock;
    private final WorkflowContext workfowContext;
    private final LambdaFunctionClient lambdaFunctionClient;
    
    public TestDecisionContext(GenericActivityClient activityClient, GenericWorkflowClient workflowClient,
            WorkflowClock workflowClock, WorkflowContext workfowContext, LambdaFunctionClient lambdaFunctionClient) {
        this.activityClient = activityClient;
        this.workflowClient = workflowClient;
        this.workflowClock = workflowClock;
        this.workfowContext = workfowContext;
        this.lambdaFunctionClient = lambdaFunctionClient;
    }

    @Override
    public GenericActivityClient getActivityClient() {
        return activityClient;
    }

    @Override
    public GenericWorkflowClient getWorkflowClient() {
        return workflowClient;
    }

    @Override
    public WorkflowClock getWorkflowClock() {
        return workflowClock;
    }

    @Override
    public WorkflowContext getWorkflowContext() {
        return workfowContext;
    }

	@Override
	public LambdaFunctionClient getLambdaFunctionClient() {
		return lambdaFunctionClient;
	}

}
