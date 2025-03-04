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

import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.DecisionContextProvider;
import com.amazonaws.services.simpleworkflow.flow.WorkflowException;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.generic.SignalExternalWorkflowParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.StartChildWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.StartChildWorkflowReply;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowDefinitionFactoryFactory;
import java.util.Collection;
import java.util.Map;


public class TestPOJOWorkflowImplementationGenericWorkflowClient implements GenericWorkflowClient {

    private final TestGenericWorkflowClient genericClient;
    private final POJOWorkflowDefinitionFactoryFactory factoryFactory;
    
    public TestPOJOWorkflowImplementationGenericWorkflowClient() {
        factoryFactory = new POJOWorkflowDefinitionFactoryFactory();
        genericClient = new TestGenericWorkflowClient(factoryFactory);
    }

    public DecisionContextProvider getDecisionContextProvider() {
        return genericClient.getDecisionContextProvider();
    }

    public void setDecisionContextProvider(DecisionContextProvider decisionContextProvider) {
        genericClient.setDecisionContextProvider(decisionContextProvider);
    }

    public Promise<StartChildWorkflowReply> startChildWorkflow(StartChildWorkflowExecutionParameters parameters) {
        return genericClient.startChildWorkflow(parameters);
    }

    public Promise<String> startChildWorkflow(String workflow, String version, String input) {
        return genericClient.startChildWorkflow(workflow, version, input);
    }

    public Promise<String> startChildWorkflow(String workflow, String version, Promise<String> input) {
        return genericClient.startChildWorkflow(workflow, version, input);
    }

    public Promise<Void> signalWorkflowExecution(SignalExternalWorkflowParameters signalParameters) {
        return genericClient.signalWorkflowExecution(signalParameters);
    }

    public void requestCancelWorkflowExecution(WorkflowExecution execution) {
        genericClient.requestCancelWorkflowExecution(execution);
    }

    public String getWorkflowState(WorkflowExecution execution) throws WorkflowException {
        return genericClient.getWorkflowState(execution);
    }

    public void continueAsNewOnCompletion(ContinueAsNewWorkflowExecutionParameters parameters) {
        genericClient.continueAsNewOnCompletion(parameters);
    }

    public String generateUniqueId() {
        return genericClient.generateUniqueId();
    }

    public void setDataConverter(DataConverter converter) {
        factoryFactory.setDataConverter(converter);
    }

    public Iterable<WorkflowType> getWorkflowTypesToRegister() {
        return factoryFactory.getWorkflowTypesToRegister();
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType)
            throws InstantiationException, IllegalAccessException {
        factoryFactory.addWorkflowImplementationType(workflowImplementationType);
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType, DataConverter converterOverride)
            throws InstantiationException, IllegalAccessException {
        factoryFactory.addWorkflowImplementationType(workflowImplementationType, converterOverride);
    }
    
    public void addWorkflowImplementationType(Class<?> workflowImplementationType, DataConverter converterOverride, Object[] constructorArgs, Map<String, Integer> maximumAllowedComponentImplementationVersions)
            throws InstantiationException, IllegalAccessException {
            factoryFactory.addWorkflowImplementationType(workflowImplementationType, converterOverride, constructorArgs, maximumAllowedComponentImplementationVersions);
        }

    public void setWorkflowImplementationTypes(Collection<Class<?>> workflowImplementationTypes)
            throws InstantiationException, IllegalAccessException {
        factoryFactory.setWorkflowImplementationTypes(workflowImplementationTypes);
    }
    
}
