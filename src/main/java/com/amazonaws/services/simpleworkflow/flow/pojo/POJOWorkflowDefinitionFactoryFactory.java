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
package com.amazonaws.services.simpleworkflow.flow.pojo;

import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.amazonaws.services.simpleworkflow.flow.WorkflowTypeRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.annotations.Execute;
import com.amazonaws.services.simpleworkflow.flow.annotations.GetState;
import com.amazonaws.services.simpleworkflow.flow.annotations.NullDataConverter;
import com.amazonaws.services.simpleworkflow.flow.annotations.Signal;
import com.amazonaws.services.simpleworkflow.flow.annotations.SkipTypeRegistration;
import com.amazonaws.services.simpleworkflow.flow.annotations.Workflow;
import com.amazonaws.services.simpleworkflow.flow.annotations.WorkflowComponentImplementationVersion;
import com.amazonaws.services.simpleworkflow.flow.annotations.WorkflowComponentImplementationVersions;
import com.amazonaws.services.simpleworkflow.flow.annotations.WorkflowRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeComponentImplementationVersion;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeImplementationOptions;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import java.beans.Expression;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class POJOWorkflowDefinitionFactoryFactory extends WorkflowDefinitionFactoryFactory {

    private static class AddedType {

        final Class<?> workflowImplementationType;

        final DataConverter converterOverride;

        final Map<String, Integer> maximumAllowedComponentImplementationVersions;

        public AddedType(Class<?> workflowImplementationType, DataConverter converterOverride,
                Map<String, Integer> maximumAllowedComponentImplementationVersions) {
            super();
            this.workflowImplementationType = workflowImplementationType;
            this.converterOverride = converterOverride;
            this.maximumAllowedComponentImplementationVersions = maximumAllowedComponentImplementationVersions;
        }

        public Class<?> getWorkflowImplementationType() {
            return workflowImplementationType;
        }

        public DataConverter getConverterOverride() {
            return converterOverride;
        }

        public Map<String, Integer> getMaximumAllowedComponentImplementationVersions() {
            return maximumAllowedComponentImplementationVersions;
        }

    }

    private DataConverter dataConverter;

    /**
     * Needed to support setting converter after types
     */
    private List<AddedType> addedTypes = new ArrayList<AddedType>();

    private List<WorkflowType> workflowTypesToRegister = new ArrayList<WorkflowType>();

    private Map<WorkflowType, POJOWorkflowDefinitionFactory> factories = new HashMap<WorkflowType, POJOWorkflowDefinitionFactory>();

    private final Collection<Class<?>> workflowImplementationTypes = new ArrayList<Class<?>>();

    public DataConverter getDataConverter() {
        return dataConverter;
    }

    public void setDataConverter(DataConverter converter) {
        this.dataConverter = converter;
        List<AddedType> typesToAdd = addedTypes;
        addedTypes = new ArrayList<AddedType>();
        for (AddedType toAdd : typesToAdd) {
            try {
                addWorkflowImplementationType(toAdd.getWorkflowImplementationType(), toAdd.getConverterOverride(),
                        null, toAdd.getMaximumAllowedComponentImplementationVersions());
            }
            catch (Exception e) {
                throw new IllegalStateException("Failure adding type " + toAdd.getWorkflowImplementationType()
                        + " after setting converter to " + converter, e);
            }
        }
    }

    @Override
    public WorkflowDefinitionFactory getWorkflowDefinitionFactory(WorkflowType workflowType) {
        return factories.get(workflowType);
    }

    @Override
    public Iterable<WorkflowType> getWorkflowTypesToRegister() {
        return workflowTypesToRegister;
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType)
            throws InstantiationException, IllegalAccessException {
        addWorkflowImplementationType(workflowImplementationType, null, null, null);
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType, DataConverter converterOverride)
            throws InstantiationException, IllegalAccessException {
        addWorkflowImplementationType(workflowImplementationType, converterOverride, null, null);
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType, DataConverter converterOverride, Object[] constructorArgs
    		, Map<String, Integer> maximumAllowedComponentImplementationVersions)
        throws InstantiationException, IllegalAccessException {
        if (workflowImplementationType.isInterface()) {
            throw new IllegalArgumentException(workflowImplementationType + " has to be a instantiatable class");
        }
        Set<Class<?>> implementedInterfaces = new HashSet<Class<?>>();
        getImplementedInterfacesAnnotatedWithWorkflow(workflowImplementationType, implementedInterfaces);
        if (implementedInterfaces.size() == 0) {
            throw new IllegalArgumentException("Workflow definition does not implement any @Workflow interface. "
                + workflowImplementationType);
        }
        for (Class<?> interfaze : implementedInterfaces) {
            addWorkflowType(interfaze, workflowImplementationType, converterOverride, constructorArgs,
                    maximumAllowedComponentImplementationVersions);
        }
        addedTypes.add(new AddedType(workflowImplementationType, converterOverride, maximumAllowedComponentImplementationVersions));
    }
    
    public void addWorkflowImplementationType(Class<?> workflowImplementationType,
            Map<String, Integer> maximumAllowedComponentImplementationVersions)
            throws InstantiationException, IllegalAccessException {
        addWorkflowImplementationType(workflowImplementationType, null, null, maximumAllowedComponentImplementationVersions);
    }

    public void setWorkflowImplementationTypes(Collection<Class<?>> workflowImplementationTypes)
            throws InstantiationException, IllegalAccessException {
        for (Class<?> type : workflowImplementationTypes) {
            addWorkflowImplementationType(type);
        }
    }

    public Collection<Class<?>> getWorkflowImplementationTypes() {
        return workflowImplementationTypes;
    }

    private void addWorkflowType(Class<?> interfaze, Class<?> workflowImplementationType, DataConverter converterOverride, Object[] constructorArgs, Map<String, Integer> maximumAllowedComponentImplementationVersions)
            throws InstantiationException, IllegalAccessException {
        Workflow workflowAnnotation = interfaze.getAnnotation(Workflow.class);
        String interfaceName = interfaze.getSimpleName();
        MethodConverterPair workflowImplementationMethod = null;
        MethodConverterPair getStateMethod = null;
        WorkflowType workflowType = null;
        WorkflowTypeRegistrationOptions registrationOptions = null;
        WorkflowTypeImplementationOptions implementationOptions = new WorkflowTypeImplementationOptions();
        Map<String, MethodConverterPair> signals = new HashMap<String, MethodConverterPair>();
        for (Method method : interfaze.getMethods()) {
            if (method.getDeclaringClass().getAnnotation(Workflow.class) == null) {
                continue;
            }
            Execute executeAnnotation = method.getAnnotation(Execute.class);
            Signal signalAnnotation = method.getAnnotation(Signal.class);
            GetState getStateAnnotation = method.getAnnotation(GetState.class);
            checkAnnotationUniqueness(method, executeAnnotation, signalAnnotation, getStateAnnotation);
            if (executeAnnotation != null) {
                if (workflowImplementationMethod != null) {
                    throw new IllegalArgumentException(
                            "Interface annotated with @Workflow is allowed to have only one method annotated with @Execute. Found "
                                    + getMethodFullName(workflowImplementationMethod.getMethod()) + " and "
                                    + getMethodFullName(method));
                }
                if (!method.getReturnType().equals(void.class) && !(Promise.class.isAssignableFrom(method.getReturnType()))) {
                    throw new IllegalArgumentException(
                            "Workflow implementation method annotated with @Execute can return only Promise or void: "
                                    + getMethodFullName(method));
                }
                if (!method.getDeclaringClass().equals(interfaze)) {
                    // If the name field of @Execute method is set, Flow will use its value as the workflow type name.
                    // Any interfaces that inherit this @Execute method will have the same workflow type name, which should not
                    // be allowed as Flow need 1:1 mapping between workflow type and workflow implementation.
                    if (executeAnnotation.name() != null && !executeAnnotation.name().isEmpty()) {
                        throw new IllegalArgumentException("Interface " + interfaze.getName()
                                + " cannot inherit workflow implementation method annotated with @Execute: "
                                + getMethodFullName(method)
                                + " which has a non-empty name: "
                                + executeAnnotation.name());
                    }
                }
                DataConverter converter = createConverter(workflowAnnotation.dataConverter(), converterOverride);
                workflowImplementationMethod = new MethodConverterPair(method, converter);
                workflowType = getWorkflowType(interfaceName, method, executeAnnotation);

                WorkflowRegistrationOptions registrationOptionsAnnotation = interfaze.getAnnotation(WorkflowRegistrationOptions.class);
                SkipTypeRegistration skipRegistrationAnnotation = interfaze.getAnnotation(SkipTypeRegistration.class);
                if (skipRegistrationAnnotation == null) {
                    if (registrationOptionsAnnotation == null) {
                        throw new IllegalArgumentException(
                                "@WorkflowRegistrationOptions is required for the interface that contains method annotated with @Execute");
                    }
                    registrationOptions = createRegistrationOptions(registrationOptionsAnnotation);
                }
                else {
                    if (registrationOptionsAnnotation != null) {
                        throw new IllegalArgumentException(
                                "@WorkflowRegistrationOptions is not allowed for the interface annotated with @SkipTypeRegistration.");
                    }
                }

                WorkflowComponentImplementationVersions implementationOptionsAnnotation = workflowImplementationType.getAnnotation(WorkflowComponentImplementationVersions.class);
                if (implementationOptionsAnnotation != null) {
                    List<WorkflowTypeComponentImplementationVersion> implementationComponentVersions = new ArrayList<WorkflowTypeComponentImplementationVersion>();
                    WorkflowComponentImplementationVersion[] componentVersionsAnnotations = implementationOptionsAnnotation.value();
                    for (WorkflowComponentImplementationVersion componentVersionAnnotation : componentVersionsAnnotations) {
                        String componentName = componentVersionAnnotation.componentName();
                        int minimumSupportedImplementationVersion = componentVersionAnnotation.minimumSupported();
                        int maximumSupportedImplementationVersion = componentVersionAnnotation.maximumSupported();
                        int maximumAllowedImplementationVersion = componentVersionAnnotation.maximumAllowed();
                        WorkflowTypeComponentImplementationVersion componentVersion = new WorkflowTypeComponentImplementationVersion(
                                componentName, minimumSupportedImplementationVersion, maximumSupportedImplementationVersion,
                                maximumAllowedImplementationVersion);
                        implementationComponentVersions.add(componentVersion);
                    }
                    implementationOptions.setImplementationComponentVersions(implementationComponentVersions);
                }
            }
            if (signalAnnotation != null) {
                String signalName = signalAnnotation.name();
                if (signalName == null || signalName.isEmpty()) {
                    signalName = method.getName();
                }
                DataConverter signalConverter = createConverter(workflowAnnotation.dataConverter(), converterOverride);
                signals.put(signalName, new MethodConverterPair(method, signalConverter));
            }
            if (getStateAnnotation != null) {
                if (getStateMethod != null) {
                    throw new IllegalArgumentException(
                            "Interface annotated with @Workflow is allowed to have only one method annotated with @GetState. Found "
                                    + getMethodFullName(getStateMethod.getMethod()) + " and " + getMethodFullName(method));
                }
                if (method.getReturnType().equals(void.class) || (Promise.class.isAssignableFrom(method.getReturnType()))) {
                    throw new IllegalArgumentException(
                            "Workflow method annotated with @GetState cannot have void or Promise return type: "
                                    + getMethodFullName(method));
                }
                DataConverter converter = createConverter(workflowAnnotation.dataConverter(), converterOverride);
                getStateMethod = new MethodConverterPair(method, converter);
            }
        }
        if (workflowImplementationMethod == null) {
            throw new IllegalArgumentException("Workflow definition does not implement any method annotated with @Execute. "
                    + workflowImplementationType);
        }
        
        
        
        POJOWorkflowImplementationFactory implementationFactory = getImplementationFactory(workflowImplementationType, interfaze,
                workflowType);
        POJOWorkflowDefinitionFactory factory = new POJOWorkflowDefinitionFactory(implementationFactory, workflowType,
                registrationOptions, implementationOptions, workflowImplementationMethod, signals, getStateMethod, constructorArgs);
        factories.put(workflowType, factory);
        workflowImplementationTypes.add(workflowImplementationType);
        if (factory.getWorkflowRegistrationOptions() != null) {
            workflowTypesToRegister.add(workflowType);
        }
        if (maximumAllowedComponentImplementationVersions != null) {
            setMaximumAllowedComponentImplementationVersions(workflowType, maximumAllowedComponentImplementationVersions);
        }
    }

    private void checkAnnotationUniqueness(Method method, Object... annotations) {
        List<Object> notNullOnes = new ArrayList<Object>();
        for (Object annotation : annotations) {
            if (annotation != null) {
                notNullOnes.add(annotation);
            }
        }
        if (notNullOnes.size() > 1) {
            throw new IllegalArgumentException("Method " + method.getName() + " is annotated with both " + notNullOnes);
        }
    }

    /**
     * Override to control how implementation is instantiated.
     * 
     * @param workflowImplementationType
     *            type that was registered with the factory
     * @param workflowInteface
     *            interface that defines external workflow contract
     * @param workflowType
     *            type of the workflow that implementation implements
     * @return factory that creates new instances of the POJO that implements
     *         workflow
     */
    protected POJOWorkflowImplementationFactory getImplementationFactory(final Class<?> workflowImplementationType,
            Class<?> workflowInteface, WorkflowType workflowType) {
        return new POJOWorkflowImplementationFactory() {

            @Override
            public Object newInstance(DecisionContext decisionContext) throws Exception {
                return workflowImplementationType.newInstance();
            }

            @Override
            public Object newInstance(DecisionContext decisionContext, Object[] constructorArgs) throws Exception {
                return new Expression(workflowImplementationType, "new", constructorArgs).getValue();
            }

            @Override
            public void deleteInstance(Object instance) {
            }
        };
    }

    /**
     * Recursively find all interfaces annotated with @Workflow that given class
     * implements. Don not include interfaces that @Workflow annotated interface
     * extends.
     */
    private void getImplementedInterfacesAnnotatedWithWorkflow(Class<?> workflowImplementationType,
            Set<Class<?>> implementedInterfaces) {
        Class<?> superClass = workflowImplementationType.getSuperclass();
        if (superClass != null) {
            getImplementedInterfacesAnnotatedWithWorkflow(superClass, implementedInterfaces);
        }
        
        Class<?>[] interfaces = workflowImplementationType.getInterfaces();
        for (Class<?> i : interfaces) {
            if (i.getAnnotation(Workflow.class) != null && !implementedInterfaces.contains(i)) {
                boolean skipAdd = removeSuperInterfaces(i, implementedInterfaces);
                if (!skipAdd) {
                    implementedInterfaces.add(i);
                }
            }
            else {
                getImplementedInterfacesAnnotatedWithWorkflow(i, implementedInterfaces);
            }
        }
    }
    
    private boolean removeSuperInterfaces(Class<?> interfaceToAdd, Set<Class<?>> implementedInterfaces) {
        boolean skipAdd = false;
        List<Class<?>> interfacesToRemove = new ArrayList<Class<?>>();
        for (Class<?> addedInterface : implementedInterfaces) {
            if (addedInterface.isAssignableFrom(interfaceToAdd)) {
                interfacesToRemove.add(addedInterface);
            }
            if (interfaceToAdd.isAssignableFrom(addedInterface)) {
                skipAdd = true;
            }
        }
        
        for (Class<?> interfaceToRemove : interfacesToRemove) {
            implementedInterfaces.remove(interfaceToRemove);
        }
        
        return skipAdd;
    }

    private static String getMethodFullName(Method m) {
        return m.getDeclaringClass().getName() + "." + m.getName();
    }

    private DataConverter createConverter(Class<? extends DataConverter> converterTypeFromAnnotation,
            DataConverter converterOverride) throws InstantiationException, IllegalAccessException {
        if (converterOverride != null) {
            return converterOverride;
        }
        if (dataConverter != null) {
            return dataConverter;
        }
        if (converterTypeFromAnnotation == null || converterTypeFromAnnotation.equals(NullDataConverter.class)) {
            return new JsonDataConverter();
        }
        return converterTypeFromAnnotation.newInstance();
    }

    protected WorkflowType getWorkflowType(String interfaceName, Method method, Execute executeAnnotation) {
        assert (method != null);
        assert (executeAnnotation != null);

        String workflowName = null;
        if (executeAnnotation.name() != null && !executeAnnotation.name().isEmpty()) {
            workflowName = executeAnnotation.name();
        }
        else {
            workflowName = interfaceName + "." + method.getName();
        }

        if (executeAnnotation.version().isEmpty()) {
            throw new IllegalArgumentException(
                    "Empty value of the required \"version\" parameter of the @Execute annotation found on "
                            + getMethodFullName(method));
        }

        return WorkflowType.builder().name(workflowName).version(executeAnnotation.version()).build();
    }

    protected WorkflowTypeRegistrationOptions createRegistrationOptions(WorkflowRegistrationOptions registrationOptionsAnnotation) {

        WorkflowTypeRegistrationOptions result = new WorkflowTypeRegistrationOptions();

        result.setDescription(emptyStringToNull(registrationOptionsAnnotation.description()));
        result.setDefaultExecutionStartToCloseTimeoutSeconds(registrationOptionsAnnotation.defaultExecutionStartToCloseTimeoutSeconds());
        result.setDefaultTaskStartToCloseTimeoutSeconds(registrationOptionsAnnotation.defaultTaskStartToCloseTimeoutSeconds());

        String taskList = registrationOptionsAnnotation.defaultTaskList();
        if (!taskList.equals(FlowConstants.USE_WORKER_TASK_LIST)) {
            result.setDefaultTaskList(taskList);
        }
        result.setDefaultChildPolicy(registrationOptionsAnnotation.defaultChildPolicy());
        String defaultLambdaRole = registrationOptionsAnnotation.defaultLambdaRole();
        if (defaultLambdaRole != null && !defaultLambdaRole.isEmpty()) {
            result.setDefaultLambdaRole(defaultLambdaRole);
        }
        return result;
    }

    private static String emptyStringToNull(String value) {
        if (value.length() == 0) {
            return null;
        }
        return value;
    }
    
    public void setMaximumAllowedComponentImplementationVersions(
            Map<WorkflowType, Map<String, Integer>> maximumAllowedImplementationVersions) {
        for (Entry<WorkflowType, Map<String, Integer>> pair : maximumAllowedImplementationVersions.entrySet()) {
            WorkflowType workflowType = pair.getKey();
            setMaximumAllowedComponentImplementationVersions(workflowType, pair.getValue());
        }
    }
 
    public void setMaximumAllowedComponentImplementationVersions(WorkflowType workflowType,
            Map<String, Integer> maximumAllowedComponentImplementationVersions) {
        POJOWorkflowDefinitionFactory factory = factories.get(workflowType);
        if (factory == null) {
            throw new IllegalArgumentException("Workflow type " + workflowType + " is not registered");
        }
        factory.setMaximumAllowedComponentImplementationVersions(maximumAllowedComponentImplementationVersions);
    }
 
    public Map<WorkflowType, Map<String, Integer>> getMaximumAllowedComponentImplementationVersions() {
        Map<WorkflowType, Map<String, Integer>> result = new HashMap<WorkflowType, Map<String, Integer>>();
        for (Entry<WorkflowType, POJOWorkflowDefinitionFactory> pair : factories.entrySet()) {
            result.put(pair.getKey(), pair.getValue().getMaximumAllowedComponentImplementationVersions());
        }
        return result;
    }
}
