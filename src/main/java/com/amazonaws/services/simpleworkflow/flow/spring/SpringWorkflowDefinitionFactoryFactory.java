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
package com.amazonaws.services.simpleworkflow.flow.spring;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.aop.framework.Advised;

import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.ExecuteActivityParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericActivityClient;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.generic.SignalExternalWorkflowParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.StartChildWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.StartChildWorkflowReply;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.worker.LambdaFunctionClient;
import com.amazonaws.services.simpleworkflow.model.ChildPolicy;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;

class SpringWorkflowDefinitionFactoryFactory extends WorkflowDefinitionFactoryFactory {

    /**
     * Used to instantiate workflow implementation with workflow scope to get
     * its class.
     */
    private static final class DummyDecisionContext extends DecisionContext {

        @Override
        public GenericActivityClient getActivityClient() {
            return new GenericActivityClient() {
                
                @Override
                public Promise<String> scheduleActivityTask(String activity, String version, Promise<String> input) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Promise<String> scheduleActivityTask(String activity, String version, String input) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Promise<String> scheduleActivityTask(ExecuteActivityParameters parameters) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public GenericWorkflowClient getWorkflowClient() {
            return new GenericWorkflowClient() {
                
                @Override
                public Promise<String> startChildWorkflow(String workflow, String version, Promise<String> input) {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public Promise<String> startChildWorkflow(String workflow, String version, String input) {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public Promise<StartChildWorkflowReply> startChildWorkflow(StartChildWorkflowExecutionParameters parameters) {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public Promise<Void> signalWorkflowExecution(SignalExternalWorkflowParameters signalParameters) {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public void requestCancelWorkflowExecution(WorkflowExecution execution) {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public String generateUniqueId() {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public void continueAsNewOnCompletion(ContinueAsNewWorkflowExecutionParameters parameters) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public WorkflowClock getWorkflowClock() {
            return new WorkflowClock() {

                @Override
                public long currentTimeMillis() {
                    return 0;
                }

                @Override
                public boolean isReplaying() {
                    return false;
                }

                @Override
                public Promise<Void> createTimer(long delaySeconds) {
                    return new Settable<Void>();
                }

                @Override
                public <T> Promise<T> createTimer(long delaySeconds, T context) {
                    return new Settable<T>();
                }

                @Override
                public <T> Promise<T> createTimer(long delaySeconds, T context, String timerId) {
                    return new Settable<T>();
                }
            };
        }

        @Override
        public WorkflowContext getWorkflowContext() {
            return new WorkflowContext() {

                @Override
                public WorkflowExecution getWorkflowExecution() {
                    WorkflowExecution result = new WorkflowExecution();
                    result.setRunId("dummyRunId");
                    result.setWorkflowId("dummyWorkflowId");
                    return result;
                }

                @Override
                public WorkflowExecution getParentWorkflowExecution() {
                    return null;
                }

                @Override
                public WorkflowType getWorkflowType() {
                    WorkflowType result = new WorkflowType();
                    result.setName("dummyName");
                    result.setVersion("dummyVersion");
                    return result;
                }

                @Override
                public boolean isCancelRequested() {
                    return false;
                }

                @Override
                public ContinueAsNewWorkflowExecutionParameters getContinueAsNewOnCompletion() {
                    return null;
                }

                @Override
                public void setContinueAsNewOnCompletion(ContinueAsNewWorkflowExecutionParameters continueParameters) {

                }

                @Override
                public List<String> getTagList() {
                    return Collections.emptyList();
                }

                @Override
                public ChildPolicy getChildPolicy() {
                    return ChildPolicy.TERMINATE;
                }

                @Override
                public String getContinuedExecutionRunId() {
                    return null;
                }

                @Override
                public long getExecutionStartToCloseTimeout() {
                    return 0;
                }

                @Override
                public String getTaskList() {
                    return "dummyTaskList";
                }

                @Override
                public boolean isImplementationVersion(String componentName, int internalVersion) {
                    return false;
                }

                @Override
                public Integer getVersion(String component) {
                    return 0;
                }

                @Override
                public int getTaskPriority() {
                    return 0;
                }

                @Override
                public String getLambdaRole() {
                    return null;
                }
            };
        }

        @Override
        public LambdaFunctionClient getLambdaFunctionClient() {
            return new LambdaFunctionClient() {
                @Override
                public Promise<String> scheduleLambdaFunction(String name, String input, long timeoutSeconds) {
                    return new Settable<String>();
                }

                @Override
                public Promise<String> scheduleLambdaFunction(String name, String input) {
                    return new Settable<String>();
                }

                @Override
                public Promise<String> scheduleLambdaFunction(String name, Promise<String> input) {
                    return new Settable<String>();
                }

                @Override
                public Promise<String> scheduleLambdaFunction(String name, Promise<String> input, long timeoutSeconds) {
                    return new Settable<String>();
                }

                @Override
                public Promise<String> scheduleLambdaFunction(String name, String input, long timeoutSeconds, String functionId) {
                    return new Settable<String>();
                }
            };
        }
    }

    private final POJOWorkflowDefinitionFactoryFactory impl = new POJOWorkflowDefinitionFactoryFactory() {

        @Override
        protected POJOWorkflowImplementationFactory getImplementationFactory(Class<?> workflowImplementationType,
                Class<?> workflowInteface, WorkflowType workflowType) {
            final Object instanceProxy = workflowImplementations.get(workflowImplementationType);
            if (instanceProxy == null) {
                throw new IllegalArgumentException("unknown workflowImplementationType: " + workflowImplementationType);
            }
            return new POJOWorkflowStubImplementationFactory(instanceProxy);
        }

    };

    private final Map<Class<?>, Object> workflowImplementations = new HashMap<Class<?>, Object>();

    @Override
    public WorkflowDefinitionFactory getWorkflowDefinitionFactory(WorkflowType workflowType) {
        return impl.getWorkflowDefinitionFactory(workflowType);
    }

    @Override
    public Iterable<WorkflowType> getWorkflowTypesToRegister() {
        return impl.getWorkflowTypesToRegister();
    }

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
        Class<? extends Object> implementationClass;
        if (workflowImplementation instanceof Advised) {
            Advised advised = (Advised) workflowImplementation;
            // Cannot use advised.getTargetClass() as the following configuration:
            //
            // @Bean(name="workflowImpl", autowire=Autowire.BY_TYPE)
            // @Scope(value="workflow", proxyMode=ScopedProxyMode.INTERFACES)
            // public MyWorkflow myWorkflow() {
            //     return new MyWorkflowImpl();
            // }
            //
            // returns MyWorkflow.class when 
            // we need MyWorkflowImpl.class
            // So the workaround is to instantiate workflow implementation which requires
            // initialization of the WorkflowScope with a context.
            try {
                WorkflowScope.setDecisionContext(new DummyDecisionContext());
                Object target = advised.getTargetSource().getTarget();
                implementationClass = target.getClass();
            }
            catch (Exception e) {
                throw new IllegalArgumentException(e);
            } finally {
                WorkflowScope.removeDecisionContext();
            }
        }
        else {
            implementationClass = workflowImplementation.getClass();
        }
        workflowImplementations.put(implementationClass, workflowImplementation);
        impl.addWorkflowImplementationType(implementationClass);
    }

    public DataConverter getDataConverter() {
        return impl.getDataConverter();
    }

    public void setDataConverter(DataConverter converter) {
        impl.setDataConverter(converter);
    }

    public void setMaximumAllowedComponentImplementationVersions(
            Map<WorkflowType, Map<String, Integer>> maximumAllowedImplementationVersions) {
        impl.setMaximumAllowedComponentImplementationVersions(maximumAllowedImplementationVersions);
    }

    public Map<WorkflowType, Map<String, Integer>> getMaximumAllowedComponentImplementationVersions() {
        return impl.getMaximumAllowedComponentImplementationVersions();
    }

}
