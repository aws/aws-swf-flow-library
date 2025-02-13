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

import com.amazonaws.services.simpleworkflow.flow.generic.GenericWorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import java.util.Map;
import software.amazon.awssdk.services.swf.model.ChildPolicy;

public abstract class WorkflowClientExternalBase implements WorkflowClientExternal {
    
    private static boolean BOOLEAN_DEFAULT = false;
    
    private static byte BYTE_DEFAULT = 0;
    
    private static char CHARACTER_DEFAULT = '\u0000';
    
    private static short SHORT_DEFAULT = 0;
    
    private static int INTEGER_DEFAULT = 0;
    
    private static long LONG_DEFAULT = 0L;
    
    private static float FLOAT_DEFAULT = 0.0f;
    
    private static double DOUBLE_DEFAULT = 0.0d;

    protected final DynamicWorkflowClientExternal dynamicWorkflowClient;

    public WorkflowClientExternalBase(WorkflowExecution workflowExecution, WorkflowType workflowType,
            StartWorkflowOptions options, DataConverter dataConverter, GenericWorkflowClientExternal genericClient) {
        this.dynamicWorkflowClient = new DynamicWorkflowClientExternalImpl(workflowExecution, workflowType, options,
                dataConverter, genericClient);
    }

    @Override
    public void requestCancelWorkflowExecution() {
        dynamicWorkflowClient.requestCancelWorkflowExecution();
    }

    @Override
    public void terminateWorkflowExecution(String reason, String details, ChildPolicy childPolicy) {
        dynamicWorkflowClient.terminateWorkflowExecution(reason, details, childPolicy);
    }

    @Override
    public DataConverter getDataConverter() {
        return dynamicWorkflowClient.getDataConverter();
    }

    @Override
    public StartWorkflowOptions getSchedulingOptions() {
        return dynamicWorkflowClient.getSchedulingOptions();
    }

    @Override
    public GenericWorkflowClientExternal getGenericClient() {
        return dynamicWorkflowClient.getGenericClient();
    }

    @Override
    public WorkflowExecution getWorkflowExecution() {
        return dynamicWorkflowClient.getWorkflowExecution();
    }
    
    @Override
    public WorkflowType getWorkflowType() {
        return dynamicWorkflowClient.getWorkflowType();
    }

    public Map<String, Integer> getImplementationVersions() {
        return dynamicWorkflowClient.getImplementationVersions();
    }

    protected void startWorkflowExecution(Object[] arguments, StartWorkflowOptions startOptionsOverride) {
        dynamicWorkflowClient.startWorkflowExecution(arguments, startOptionsOverride);
    }

    protected void startWorkflowExecution(Object[] arguments) {
        dynamicWorkflowClient.startWorkflowExecution(arguments);
    }

    protected void signalWorkflowExecution(String signalName, Object[] arguments) {
        dynamicWorkflowClient.signalWorkflowExecution(signalName, arguments);
    }
    
    @SuppressWarnings({ "unchecked" })
    protected<T> T defaultPrimitiveValue(Class<T> clazz) {
        Object returnValue = null;
        if (clazz.equals(Boolean.TYPE)) {
            returnValue = BOOLEAN_DEFAULT;
        } else if (clazz.equals(Byte.TYPE)) {
            returnValue = BYTE_DEFAULT;
        } else if (clazz.equals(Character.TYPE)) {
            returnValue = CHARACTER_DEFAULT;
        } else if (clazz.equals(Short.TYPE)) {
            returnValue = SHORT_DEFAULT;
        } else if (clazz.equals(Integer.TYPE)) {
            returnValue = INTEGER_DEFAULT;
        } else if (clazz.equals(Long.TYPE)) {
            returnValue = LONG_DEFAULT;
        } else if (clazz.equals(Float.TYPE)) {
            returnValue = FLOAT_DEFAULT;
        } else if (clazz.equals(Double.TYPE)) {
            returnValue = DOUBLE_DEFAULT;
        } else {
            throw new IllegalArgumentException("Type not supported: " + clazz);
        }
        
        return (T)returnValue;
    }
}
