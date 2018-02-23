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
package com.amazonaws.services.simpleworkflow.flow.generic;
 
/**
 * @author fateev
 * 
 */
public class WorkflowTypeComponentImplementationVersion {
 
    private final String componentName;
 
    private final int minimumSupported;
 
    private final int maximumSupported;
 
    private int maximumAllowed;
 
    public WorkflowTypeComponentImplementationVersion(String componentName, int minimumSupported, int maximumSupported,
            int maximumAllowed) {
        this.componentName = componentName;
        this.minimumSupported = minimumSupported;
        this.maximumSupported = maximumSupported;
        this.maximumAllowed = maximumAllowed;
    }
 
    public String getComponentName() {
        return componentName;
    }
 
    /**
     * Minimum version supported for code replay
     */
    public int getMinimumSupported() {
        return minimumSupported;
    }
 
    /**
     * Maximum version supported for code replay
     */
    public int getMaximumSupported() {
        return maximumSupported;
    }
 
    public int getMaximumAllowed() {
        return maximumAllowed;
    }
 
    /**
     * Maximum version allowed for a newly executed code
     */
    public void setMaximumAllowed(int maximumAllowed) {
        this.maximumAllowed = maximumAllowed;
    }
    
}