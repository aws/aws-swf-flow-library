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

import software.amazon.awssdk.services.swf.model.ChildPolicy;

public class WorkflowTypeRegistrationOptions {

    private ChildPolicy defaultChildPolicy = ChildPolicy.TERMINATE;
    
    private long defaultExecutionStartToCloseTimeoutSeconds;
    
    private long defaultTaskStartToCloseTimeoutSeconds;
    
    private String defaultTaskList;
    
    private String description;
    
    private int defaultTaskPriority;

    private String defaultLambdaRole;

    public ChildPolicy getDefaultChildPolicy() {
        return defaultChildPolicy;
    }
    
    public void setDefaultChildPolicy(ChildPolicy defaultChildPolicy) {
        this.defaultChildPolicy = defaultChildPolicy;
    }
    
    public long getDefaultExecutionStartToCloseTimeoutSeconds() {
        return defaultExecutionStartToCloseTimeoutSeconds;
    }
    
    public void setDefaultExecutionStartToCloseTimeoutSeconds(long defaultExecutionStartToCloseTimeoutSeconds) {
        this.defaultExecutionStartToCloseTimeoutSeconds = defaultExecutionStartToCloseTimeoutSeconds;
    }
    
    /**
     * Default Workflow TaskList. <code>null</code> means to use {@link WorkflowWorker} task list.
     * TaskList with "NO_DEFAULT_TASK_LIST" name means that no default task list is registered.
     *
     * @return String - the default task list name, null if using WorkflowWorker task list,
     * or "NO_DEFAULT_TASK_LIST" if no default task list is registered
     */
    public String getDefaultTaskList() {
        return defaultTaskList;
    }
    
    public void setDefaultTaskList(String defaultTaskList) {
        this.defaultTaskList = defaultTaskList;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public long getDefaultTaskStartToCloseTimeoutSeconds() {
        return defaultTaskStartToCloseTimeoutSeconds;
    }

    public void setDefaultTaskStartToCloseTimeoutSeconds(long defaultTaskStartToCloseTimeoutSeconds) {
        this.defaultTaskStartToCloseTimeoutSeconds = defaultTaskStartToCloseTimeoutSeconds;
    }

    public int getDefaultTaskPriority() {
        return defaultTaskPriority;
    }

    public void setDefaultTaskPriority(int defaultTaskPriority) {
        this.defaultTaskPriority = defaultTaskPriority;
    }

    public String getDefaultLambdaRole() {
        return defaultLambdaRole;
    }

    public void setDefaultLambdaRole(String defaultLambdaRole) {
        this.defaultLambdaRole = defaultLambdaRole;
    }

    @Override
    public String toString() {
        return "WorkflowVersionRegistrationOptions [defaultTaskList=" + defaultTaskList
                + ", defaultExecutionStartToCloseTimeoutSeconds=" + defaultExecutionStartToCloseTimeoutSeconds
                + ", defaultTaskList=" + defaultTaskList
                + ", description=" + description 
                + ", defaultTaskStartToCloseTimeoutSeconds=" + defaultTaskStartToCloseTimeoutSeconds
                + ", defaultTaskPriority=" + defaultTaskPriority
                + ", defaultLambdaRole=" + defaultLambdaRole
                + "]";
    }

}
