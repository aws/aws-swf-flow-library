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
package com.amazonaws.services.simpleworkflow.flow.pojo;

import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.amazonaws.services.simpleworkflow.flow.WorkflowTypeRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeComponentImplementationVersion;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeImplementationOptions;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import com.amazonaws.services.simpleworkflow.flow.worker.CurrentDecisionContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

class POJOWorkflowDefinitionFactory extends WorkflowDefinitionFactory {

    private final DataConverter converter = new JsonDataConverter();

    private final WorkflowType workflowType;

    private final WorkflowTypeRegistrationOptions registrationOptions;

    private final WorkflowTypeImplementationOptions implementationOptions;
    
    private final POJOWorkflowImplementationFactory implementationFactory;

    private final MethodConverterPair workflowImplementationMethod;

    private final MethodConverterPair getStateMethod;

    private final Map<String, MethodConverterPair> signals;

    private final Object[] constructorArgs;

    public POJOWorkflowDefinitionFactory(POJOWorkflowImplementationFactory implementationFactory, WorkflowType workflowType,
    		WorkflowTypeRegistrationOptions registrationOptions, WorkflowTypeImplementationOptions implementationOptions,
            MethodConverterPair workflowImplementationMethod, Map<String, MethodConverterPair> signals,
            MethodConverterPair getStateMethod, Object[] constructorArgs) {
        this.implementationFactory = implementationFactory;
        this.workflowType = workflowType;
        this.registrationOptions = registrationOptions;
        this.workflowImplementationMethod = workflowImplementationMethod;
        this.signals = signals;
        this.getStateMethod = getStateMethod;
        this.implementationOptions = implementationOptions;
        this.constructorArgs = constructorArgs;
    }

    @Override
    public WorkflowType getWorkflowType() {
        return workflowType;
    }

    @Override
    public WorkflowTypeRegistrationOptions getWorkflowRegistrationOptions() {
        return registrationOptions;
    }
    
    @Override
    public WorkflowTypeImplementationOptions getWorkflowImplementationOptions() {
        return implementationOptions;
    }

    @Override
    public WorkflowDefinition getWorkflowDefinition(DecisionContext context) throws Exception {
        if (implementationFactory == null) {
            return null;
        }
        CurrentDecisionContext.set(context);
        Object workflowDefinitionObject;
        if (constructorArgs == null) {
            workflowDefinitionObject = implementationFactory.newInstance(context);
        }
        else {
            workflowDefinitionObject = implementationFactory.newInstance(context, constructorArgs);
        }
        return new POJOWorkflowDefinition(workflowDefinitionObject, workflowImplementationMethod, signals, getStateMethod,
                converter, context);
    }

    @Override
    public void deleteWorkflowDefinition(WorkflowDefinition instance) {
        POJOWorkflowDefinition definition = (POJOWorkflowDefinition) instance;
        implementationFactory.deleteInstance(definition.getImplementationInstance());
        CurrentDecisionContext.unset();
    }
    
    public void setMaximumAllowedComponentImplementationVersions(Map<String, Integer> componentVersions) {
        componentVersions = new HashMap<String, Integer>(componentVersions);
        List<WorkflowTypeComponentImplementationVersion> options = implementationOptions.getImplementationComponentVersions();
        Map<String, WorkflowTypeComponentImplementationVersion> implementationOptionsMap = new HashMap<String, WorkflowTypeComponentImplementationVersion>();
        for (WorkflowTypeComponentImplementationVersion implementationVersion : options) {
            String componentName = implementationVersion.getComponentName();
            implementationOptionsMap.put(componentName, implementationVersion);
        }
 
        for (Entry<String, Integer> pair : componentVersions.entrySet()) {
            String componentName = pair.getKey();
            int maximumAllowed = pair.getValue();
            WorkflowTypeComponentImplementationVersion implementationOption = implementationOptionsMap.get(componentName);
            if (implementationOption != null) {
                implementationOption.setMaximumAllowed(maximumAllowed);
            }
            else {
                implementationOption = new WorkflowTypeComponentImplementationVersion(componentName, maximumAllowed,
                        maximumAllowed, maximumAllowed);
                implementationOptions.getImplementationComponentVersions().add(implementationOption);
            }
        }
    }
 
    public Map<String, Integer> getMaximumAllowedComponentImplementationVersions() {
        List<WorkflowTypeComponentImplementationVersion> options = implementationOptions.getImplementationComponentVersions();
        Map<String, Integer> result = new HashMap<String, Integer>();
        for (WorkflowTypeComponentImplementationVersion implementationVersion : options) {
            String componentName = implementationVersion.getComponentName();
            result.put(componentName, implementationVersion.getMaximumAllowed());
        }
        return result;
    }
}
