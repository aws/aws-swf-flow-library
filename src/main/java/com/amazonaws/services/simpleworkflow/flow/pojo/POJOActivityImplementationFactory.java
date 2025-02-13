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
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.amazonaws.services.simpleworkflow.flow.annotations.Activities;
import com.amazonaws.services.simpleworkflow.flow.annotations.Activity;
import com.amazonaws.services.simpleworkflow.flow.annotations.ActivityCompletionRetryOptions;
import com.amazonaws.services.simpleworkflow.flow.annotations.ActivityExecutionOptions;
import com.amazonaws.services.simpleworkflow.flow.annotations.ActivityRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.annotations.ManualActivityCompletion;
import com.amazonaws.services.simpleworkflow.flow.annotations.NullDataConverter;
import com.amazonaws.services.simpleworkflow.flow.annotations.SkipTypeRegistration;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementation;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeCompletionRetryOptions;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeExecutionOptions;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeRegistrationOptions;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class POJOActivityImplementationFactory extends ActivityImplementationFactory {

    private static class ParentInterfaceOptions {

        private String version;

        private String prefix;

        private ActivityRegistrationOptions registrationOptions;

        private boolean skipRegistration;

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public ActivityRegistrationOptions getRegistrationOptions() {
            return registrationOptions;
        }

        public void setRegistrationOptions(ActivityRegistrationOptions options) {
            if (options != null) {
                setSkipRegistration(false);
            }
            this.registrationOptions = options;
        }

        public boolean isSkipRegistration() {
            return skipRegistration;
        }

        public void setSkipRegistration(boolean skipRegistration) {
            if (skipRegistration) {
                registrationOptions = null;
            }
            this.skipRegistration = skipRegistration;
        }

    }

    private static class AddedType {

        final Object activitiesImplementation;

        final DataConverter converter;

        public AddedType(Object activitiesImplementation, DataConverter converter) {
            super();
            this.activitiesImplementation = activitiesImplementation;
            this.converter = converter;
        }

        public Object getActivitiesImplementation() {
            return activitiesImplementation;
        }

        public DataConverter getConverter() {
            return converter;
        }

    }

    /**
     * Needed to support setting converter after activities implementation
     */
    private List<AddedType> addedTypes = new ArrayList<AddedType>();

    private List<ActivityType> activityTypesToRegister = new ArrayList<ActivityType>();

    private Map<ActivityType, POJOActivityImplementation> implementationsMap = new HashMap<ActivityType, POJOActivityImplementation>();

    private DataConverter dataConverter;

    public POJOActivityImplementationFactory() {
        super();
    }

    public POJOActivityImplementationFactory(Iterable<Object> activityImplementationObjects)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        this();
        addActivitiesImplementations(activityImplementationObjects, null);
    }

    public POJOActivityImplementationFactory(Iterable<Object> activityImplementationObjects, DataConverter dataConverter)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        this();
        addActivitiesImplementations(activityImplementationObjects, dataConverter);
    }

    public DataConverter getDataConverter() {
        return dataConverter;
    }

    public void setDataConverter(DataConverter dataConverter) {
        if (dataConverter == null) {
            throw new IllegalArgumentException("null dataConverter");
        }
        this.dataConverter = dataConverter;
        List<AddedType> typesToAdd = addedTypes;
        addedTypes = new ArrayList<AddedType>();
        activityTypesToRegister.clear();
        implementationsMap.clear();
        for (AddedType toAdd : typesToAdd) {
            try {
                addActivitiesImplementation(toAdd.getActivitiesImplementation(), toAdd.getConverter());
            }
            catch (Exception e) {
                throw new IllegalStateException("Failure adding activity " + toAdd.getActivitiesImplementation()
                        + " after setting converter to " + dataConverter, e);
            }
        }
    }

    public void setActivitiesImplementations(Iterable<Object> activitiesImplementations)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        addActivitiesImplementations(activitiesImplementations, null);
    }

    public Iterable<Object> getActivitiesImplementations() {
        List<Object> result = new ArrayList<Object>();
        for (POJOActivityImplementation impl : implementationsMap.values()) {
            result.add(impl.getActivitiesImplementation());
        }
        return result;
    }

    public List<ActivityType> addActivitiesImplementations(Iterable<Object> activitiesImplementations)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        return addActivitiesImplementations(activitiesImplementations, null);
    }

    public List<ActivityType> addActivitiesImplementations(Iterable<Object> activitiesImplementations, DataConverter dataConverter)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        List<ActivityType> result = new ArrayList<ActivityType>();
        for (Object activityImplementationObject : activitiesImplementations) {
            result.addAll(addActivitiesImplementation(activityImplementationObject, dataConverter));
        }
        return result;
    }

    public List<ActivityType> addActivitiesImplementation(Object activitiesImplementation)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        return addActivitiesImplementation(activitiesImplementation, null);
    }

    public List<ActivityType> addActivitiesImplementation(Object activitiesImplementation, DataConverter converter)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        if (activitiesImplementation == null) {
            throw new IllegalArgumentException("activitiesImplementation is null.");
        }
        Set<ActivityType> result = new HashSet<ActivityType>();
        Set<Class<?>> activitiesInterfaces = new HashSet<Class<?>>();
        getImplementedInterfacesAnnotatedWithActivities(activitiesImplementation.getClass(), activitiesInterfaces);
        if (activitiesInterfaces.size() == 0) {
            throw new IllegalArgumentException(
                    "Activity implementation object does not implement any interface annotated with @Activities: "
                            + activitiesImplementation.getClass());
        }
        for (Class<?> interfaze : activitiesInterfaces) {
            Map<String, Method> methods = new HashMap<String, Method>();
            ParentInterfaceOptions parentOptions = new ParentInterfaceOptions();
            addActivities(activitiesImplementation, interfaze, methods, parentOptions, converter, result);
        }
        addedTypes.add(new AddedType(activitiesImplementation, converter));
        return new ArrayList<ActivityType>(result);
    }

    private void addActivities(Object implementation, Class<?> interfaze, Map<String, Method> methods,
            ParentInterfaceOptions parentOptions, DataConverter converter, Set<ActivityType> addedTypes)
            throws SecurityException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Activities activitiesAnnotation = interfaze.getAnnotation(Activities.class);
        for (Class<?> parent : interfaze.getInterfaces()) {
            addActivities(implementation, parent, methods, parentOptions, converter, addedTypes);
        }
        if (activitiesAnnotation != null) {
            String interfaceName = interfaze.getSimpleName();
            if (!nullOrEmpty(activitiesAnnotation.activityNamePrefix())) {
                parentOptions.setPrefix(activitiesAnnotation.activityNamePrefix());
            }
            if (!nullOrEmpty(activitiesAnnotation.version())) {
                parentOptions.setVersion(activitiesAnnotation.version());
            }
            converter = (converter == null) ? createConverter(activitiesAnnotation.dataConverter()) : converter;
            ActivityRegistrationOptions interfaceRegistrationOptionsAnnotation = interfaze.getAnnotation(ActivityRegistrationOptions.class);
            SkipTypeRegistration interfaceSkipTypeRegistrationAnnotation = interfaze.getAnnotation(SkipTypeRegistration.class);
            if (interfaceRegistrationOptionsAnnotation != null) {
                if (interfaceSkipTypeRegistrationAnnotation != null) {
                    throw new IllegalArgumentException(
                            "@ActivityRegistrationOptions is not allowed for the interface annotated with @SkipTypeRegistration.");

                }
                parentOptions.setRegistrationOptions(interfaceRegistrationOptionsAnnotation);
            }
            else if (interfaceSkipTypeRegistrationAnnotation != null) {
                parentOptions.setSkipRegistration(true);
            }
            for (Method method : interfaze.getMethods()) {
                if (!method.getDeclaringClass().equals(interfaze)) {
                    continue;
                }
                Activity activityAnnotation = method.getAnnotation(Activity.class);
                ActivityType activityType = getActivityType(interfaceName, method, activityAnnotation, parentOptions);
                ActivityRegistrationOptions registrationOptionsAnnotation = method.getAnnotation(ActivityRegistrationOptions.class);
                SkipTypeRegistration skipTypeRegistrationAnnotation = interfaze.getAnnotation(SkipTypeRegistration.class);
                ActivityTypeRegistrationOptions registrationOptions = null;
                if (skipTypeRegistrationAnnotation != null) {
                    if (registrationOptionsAnnotation != null) {
                        throw new IllegalArgumentException(
                                "@ActivityRegistrationOptions is not allowed for the method annotated with @SkipTypeRegistration: "
                                        + method);
                    }
                }
                else {
                    if (registrationOptionsAnnotation != null || parentOptions.getRegistrationOptions() != null) {
                        POJOActivityImplementation existingImplementation = implementationsMap.get(activityType);
                        if (existingImplementation != null && !addedTypes.contains(activityType)) {
                            String message = "Duplicate declaration for activity type=" + activityType.getName() + ", version="
                                    + activityType.getVersion() + ": " + existingImplementation.getMethod() + " and " + method;
                            throw new IllegalArgumentException(message);
                        }
                        registrationOptions = createRegistrationOptions(registrationOptionsAnnotation,
                                parentOptions.getRegistrationOptions());
                    }
                    else if (!parentOptions.isSkipRegistration()) {
                        throw new IllegalArgumentException(
                                "No @ActivityRegistrationOptions found either on interface or method for " + method);
                    }
                }
                //TODO: support methods defined in parents as well as overrides
                if (!addedTypes.contains(activityType)) {
                    Method activityImplementationMethod = implementation.getClass().getMethod(method.getName(),
                            method.getParameterTypes());
                    ActivityTypeExecutionOptions executionOptions = createExecutionOptions(activityType,
                            activityImplementationMethod);

                    POJOActivityImplementation activityImplementation = new POJOActivityImplementation(implementation, method,
                            registrationOptions, executionOptions, converter);
                    activityTypesToRegister.add(activityType);
                    addedTypes.add(activityType);
                    implementationsMap.put(activityType, activityImplementation);
                }
            }
        }

    }

    @Override
    public Iterable<ActivityType> getActivityTypesToRegister() {
        return activityTypesToRegister;
    }

    @Override
    public ActivityImplementation getActivityImplementation(ActivityType activityType) {
        return implementationsMap.get(activityType);
    }

    private DataConverter createConverter(Class<? extends DataConverter> converterType)
            throws InstantiationException, IllegalAccessException {
        if (dataConverter != null) {
            return dataConverter;
        }
        if (converterType == null || converterType.equals(NullDataConverter.class)) {
            return new JsonDataConverter();
        }
        return converterType.newInstance();
    }

    private static ActivityType getActivityType(String interfaceName, Method activity, Activity activityAnnotation,
            ParentInterfaceOptions parentOptions) {

        String activityName = null;
        String activityVersion = null;
        if (activityAnnotation != null) {
            if (!nullOrEmpty(activityAnnotation.name())) {
                activityName = activityAnnotation.name();
            }
            if (!nullOrEmpty(activityAnnotation.version())) {
                activityVersion = activityAnnotation.version();
            }

        }
        if (activityName == null) {
            if (!nullOrEmpty(parentOptions.getPrefix())) {
                activityName = parentOptions.getPrefix() + activity.getName();
            }
            else {
                activityName = interfaceName + "." + activity.getName();
            }
        }
        if (activityVersion == null) {
            if (!nullOrEmpty(parentOptions.getVersion())) {
                activityVersion = parentOptions.getVersion();
            }
            else {
                throw new IllegalArgumentException("No version found for activity defined by " + activity);
            }
        }
        return ActivityType.builder().name(activityName).version(activityVersion).build();
    }

    private static boolean nullOrEmpty(String nameFromAnnotation) {
        return nameFromAnnotation == null || nameFromAnnotation.isEmpty();
    }

    /**
     * Recursively find all interfaces annotated with @Activities that given
     * class implements. Do not include interfaces that @Activities annotated
     * interface extends.
     */
    private void getImplementedInterfacesAnnotatedWithActivities(Class<?> implementationType, Set<Class<?>> implementedInterfaces) {
        Class<?> superClass = implementationType.getSuperclass();
        if (superClass != null) {
            getImplementedInterfacesAnnotatedWithActivities(superClass, implementedInterfaces);
        }

        Class<?>[] interfaces = implementationType.getInterfaces();
        for (Class<?> i : interfaces) {
            if (i.getAnnotation(Activities.class) != null && !implementedInterfaces.contains(i)) {
                boolean skipAdd = removeSuperInterfaces(i, implementedInterfaces);
                if (!skipAdd) {
                    implementedInterfaces.add(i);
                }
            }
            else {
                getImplementedInterfacesAnnotatedWithActivities(i, implementedInterfaces);
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

    private static ActivityTypeRegistrationOptions createRegistrationOptions(ActivityRegistrationOptions registrationOptions,
            ActivityRegistrationOptions parentRegistrationOptions) {

        ActivityRegistrationOptions registrationOptionsAnnotation = registrationOptions != null ? registrationOptions
                : parentRegistrationOptions;

        ActivityTypeRegistrationOptions result = new ActivityTypeRegistrationOptions();

        result.setDescription(emptyStringToNull(registrationOptionsAnnotation.description()));

        long taskHeartbeatTimeoutSeconds = registrationOptionsAnnotation.defaultTaskHeartbeatTimeoutSeconds();
        if (taskHeartbeatTimeoutSeconds > FlowConstants.USE_REGISTERED_DEFAULTS) {
            result.setDefaultTaskHeartbeatTimeoutSeconds(taskHeartbeatTimeoutSeconds);
        }

        long taskScheduleToCloseTimeoutSeconds = registrationOptionsAnnotation.defaultTaskScheduleToCloseTimeoutSeconds();
        if (taskScheduleToCloseTimeoutSeconds > FlowConstants.USE_REGISTERED_DEFAULTS) {
            result.setDefaultTaskScheduleToCloseTimeoutSeconds(taskScheduleToCloseTimeoutSeconds);
        }

        long taskScheduleToStartTimeoutSeconds = registrationOptionsAnnotation.defaultTaskScheduleToStartTimeoutSeconds();
        if (taskScheduleToStartTimeoutSeconds > FlowConstants.USE_REGISTERED_DEFAULTS) {
            result.setDefaultTaskScheduleToStartTimeoutSeconds(taskScheduleToStartTimeoutSeconds);
        }

        long taskStartToCloseTimeoutSeconds = registrationOptionsAnnotation.defaultTaskStartToCloseTimeoutSeconds();
        if (taskStartToCloseTimeoutSeconds > FlowConstants.USE_REGISTERED_DEFAULTS) {
            result.setDefaultTaskStartToCloseTimeoutSeconds(taskStartToCloseTimeoutSeconds);
        }

        String taskList = registrationOptionsAnnotation.defaultTaskList();
        if (!taskList.equals(FlowConstants.USE_WORKER_TASK_LIST)) {
            result.setDefaultTaskList(taskList);
        }
        else if (taskList.equals(FlowConstants.NO_DEFAULT_TASK_LIST)) {
            result.setDefaultTaskList(null);
        }

        return result;
    }

    private static ActivityTypeExecutionOptions createExecutionOptions(ActivityType activityType, Method activityImplementation) {
        assert (activityType != null);
        ActivityTypeExecutionOptions executionOptions = new ActivityTypeExecutionOptions();
        if (activityImplementation != null) {
            ManualActivityCompletion manualCompletion = activityImplementation.getAnnotation(ManualActivityCompletion.class);

            // Iterate our parent classes as well
            Class<?> cl = activityImplementation.getDeclaringClass();
            while(manualCompletion == null) {
                cl = cl.getSuperclass();
                if(cl == null || cl.equals(Object.class)) {
                        break;
                }
                
                try {
                        Method equivalentMethod = cl.getDeclaredMethod(activityImplementation.getName(), activityImplementation.getParameterTypes());
                        if (equivalentMethod != null) {
                                manualCompletion = equivalentMethod.getAnnotation(ManualActivityCompletion.class);
                        }
                } catch (NoSuchMethodException e) {
                        // No problem
                }
            }
            
            executionOptions.setManualActivityCompletion(manualCompletion != null);
            ActivityExecutionOptions options = activityImplementation.getAnnotation(ActivityExecutionOptions.class);
            if (options == null) {
                //TODO: Check superclasses for the annotation
                options = activityImplementation.getDeclaringClass().getAnnotation(ActivityExecutionOptions.class);
            }
            if (options != null) {
                ActivityCompletionRetryOptions completionRetryOptions = options.completionRetryOptions();
                ActivityTypeCompletionRetryOptions typeCompletionRetryOptions = completionRetryOptionsFromAnnotation(completionRetryOptions);
                executionOptions.setCompletionRetryOptions(typeCompletionRetryOptions);
                ActivityCompletionRetryOptions failureRetryOptions = options.failureRetryOptions();
                ActivityTypeCompletionRetryOptions typeFailureRetryOptions = completionRetryOptionsFromAnnotation(failureRetryOptions);
                executionOptions.setFailureRetryOptions(typeFailureRetryOptions);
            }
        }

        return executionOptions;
    }

    public static ActivityTypeCompletionRetryOptions completionRetryOptionsFromAnnotation(
            ActivityCompletionRetryOptions failureRetryOptions) {
        ActivityTypeCompletionRetryOptions typeFailureRetryOptions = new ActivityTypeCompletionRetryOptions();
        typeFailureRetryOptions.setInitialRetryIntervalSeconds(failureRetryOptions.initialRetryIntervalSeconds());
        typeFailureRetryOptions.setMaximumRetryIntervalSeconds(failureRetryOptions.maximumRetryIntervalSeconds());
        typeFailureRetryOptions.setMinimumAttempts(failureRetryOptions.minimumAttempts());
        typeFailureRetryOptions.setMaximumAttempts(failureRetryOptions.maximumAttempts());
        typeFailureRetryOptions.setBackoffCoefficient(failureRetryOptions.backoffCoefficient());
        typeFailureRetryOptions.setRetryExpirationSeconds(failureRetryOptions.retryExpirationSeconds());
        return typeFailureRetryOptions;
    }

    private static String emptyStringToNull(String value) {
        if (value.length() == 0) {
            return null;
        }
        return value;
    }

}
