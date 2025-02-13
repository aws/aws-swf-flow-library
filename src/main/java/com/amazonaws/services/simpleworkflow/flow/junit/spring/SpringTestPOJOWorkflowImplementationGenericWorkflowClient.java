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
package com.amazonaws.services.simpleworkflow.flow.junit.spring;

import java.util.HashMap;
import java.util.Map;

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
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.spring.POJOWorkflowStubImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.test.TestGenericWorkflowClient;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 *
 */
public class SpringTestPOJOWorkflowImplementationGenericWorkflowClient implements GenericWorkflowClient {

    private final TestGenericWorkflowClient genericClient;
    
    public SpringTestPOJOWorkflowImplementationGenericWorkflowClient() {
        genericClient = new TestGenericWorkflowClient();
        genericClient.setFactoryFactory(new POJOWorkflowDefinitionFactoryFactory() {

            @Override
            protected POJOWorkflowImplementationFactory getImplementationFactory(Class<?> workflowImplementationType,
                    Class<?> workflowInteface, WorkflowType workflowType) {
                final Object instanceProxy = workflowImplementations.get(workflowImplementationType);
                if (instanceProxy == null) {
                    throw new IllegalArgumentException("unknown workflowImplementationType: " + workflowImplementationType);
                }
                return new POJOWorkflowStubImplementationFactory(instanceProxy);
            }
        });
    }

    private final Map<Class<?>, Object> workflowImplementations = new HashMap<Class<?>, Object>();

    public void setWorkflowImplementations(Iterable<Object> workflowImplementations)
            throws InstantiationException, IllegalAccessException {
        for (Object workflowImplementation : workflowImplementations) {
            addWorkflowImplementation(workflowImplementation);
        }
    }

    public Iterable<Object> getWorkflowImplementations() {
        return workflowImplementations.values();
    }

    public void addWorkflowImplementation(Object workflowImplementation) throws InstantiationException, IllegalAccessException {
        Class<? extends Object> implementationClass = workflowImplementation.getClass();
        workflowImplementations.put(implementationClass, workflowImplementation);
        getFactoryFactory().addWorkflowImplementationType(implementationClass);
    }

    private POJOWorkflowDefinitionFactoryFactory getFactoryFactory() {
        return ((POJOWorkflowDefinitionFactoryFactory)genericClient.getFactoryFactory());
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

    public DataConverter getDataConverter() {
        return getFactoryFactory().getDataConverter();
    }

    public void setDataConverter(DataConverter converter) {
        getFactoryFactory().setDataConverter(converter);
    }

    public Iterable<WorkflowType> getWorkflowTypesToRegister() {
        return getFactoryFactory().getWorkflowTypesToRegister();
    }
    
}
