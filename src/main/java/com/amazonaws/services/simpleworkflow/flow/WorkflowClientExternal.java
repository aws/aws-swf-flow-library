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

public interface WorkflowClientExternal {
    
    public void requestCancelWorkflowExecution();

    public void terminateWorkflowExecution(String reason, String details, ChildPolicy childPolicy);
    
    public DataConverter getDataConverter();

    public StartWorkflowOptions getSchedulingOptions();
    
    public GenericWorkflowClientExternal getGenericClient();
    
    public WorkflowExecution getWorkflowExecution();
    
    public WorkflowType getWorkflowType();
    
    /**
     * @see WorkflowContext#isImplementationVersion(String, int)
     *
     * @return - map of component names to their implementation versions,
     *      where the key is the component name and the value is its current version number
     */
    public Map<String, Integer> getImplementationVersions();
    
}
