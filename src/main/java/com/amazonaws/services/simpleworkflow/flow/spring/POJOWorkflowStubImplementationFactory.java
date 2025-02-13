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
package com.amazonaws.services.simpleworkflow.flow.spring;

import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowImplementationFactory;

/**
 * A stub implementation of the @see{POJOWorkflowImplementationFactory}
 * Used for Spring injection proxying
 *
 * @author nicholasterry
 */
public class POJOWorkflowStubImplementationFactory implements POJOWorkflowImplementationFactory {

    private Object instanceProxy;

    public POJOWorkflowStubImplementationFactory(Object instanceProxy) {
        this.instanceProxy = instanceProxy;
    }

    @Override
    public Object newInstance(DecisionContext decisionContext) throws Exception {
        WorkflowScope.setDecisionContext(decisionContext);
        return instanceProxy;
    }

    @Override
    public void deleteInstance(Object instance) {
        WorkflowScope.removeDecisionContext();
    }

    @Override
    public Object newInstance(DecisionContext decisionContext, Object[] constructorArgs) throws Exception {
        WorkflowScope.setDecisionContext(decisionContext);
        return instanceProxy;
    }
}
