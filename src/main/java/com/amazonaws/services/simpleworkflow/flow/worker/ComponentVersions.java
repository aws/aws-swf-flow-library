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
package com.amazonaws.services.simpleworkflow.flow.worker;
 
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
 
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeComponentImplementationVersion;
 
/**
 * @author fateev
 * 
 */
class ComponentVersions {
 
    private final Map<String, ComponentVersion> versions = new HashMap<String, ComponentVersion>();
 
    void setVersionFromHistory(String component, int version) {
        ComponentVersion cv = getComponentVersion(component);
        cv.setVersionFromHistory(version);
    }
 
    public Map<String, Integer> getVersionsToSave() {
        Map<String, Integer> result = new HashMap<String, Integer>();
        for (Entry<String, ComponentVersion> pair : versions.entrySet()) {
            ComponentVersion version = pair.getValue();
            int currentVersion = version.getCurrentVersion();
            if (currentVersion > 0) {
                result.put(pair.getKey(), currentVersion);
            }
        }
        return result;
    }
 
    private ComponentVersion getComponentVersion(String component) {
        ComponentVersion cv = versions.get(component);
        if (cv == null) {
            cv = new ComponentVersion(component);
            versions.put(component, cv);
        }
        return cv;
    }
 
    public boolean isVersion(String component, int version, boolean replaying) {
        if (component.contains(AsyncDecisionTaskHandler.COMPONENT_VERSION_SEPARATOR)) {
            throw new IncompatibleWorkflowDefinition("component name cannot contain character with code="
                    + Pattern.quote(AsyncDecisionTaskHandler.COMPONENT_VERSION_SEPARATOR));
        }
        ComponentVersion cv = versions.get(component);
        if (cv == null) {
            cv = new ComponentVersion(component);
            versions.put(component, cv);
            if (cv.isVersion(version, replaying)) {
                return true;
            }
            return false;
        }
        else {
            return cv.isVersion(version, replaying);
        }
    }
 
    /**
     * @param component
     * @return
     */
    public Integer getCurrentVersion(String component) {
        ComponentVersion cv = versions.get(component);
        if (cv == null) {
            return null;
        }
        return cv.getCurrentVersion();
    }
 
    public void setWorkflowImplementationComponentVersions(List<WorkflowTypeComponentImplementationVersion> versions) {
        for (WorkflowTypeComponentImplementationVersion version : versions) {
            String componentName = version.getComponentName();
            ComponentVersion componentVersion = getComponentVersion(componentName);
            componentVersion.setMaximumAllowedImplementationVersion(version.getMaximumAllowed());
            componentVersion.setMaximumSupportedImplementationVersion(version.getMaximumSupported());
            componentVersion.setMinimumSupportedImplementationVersion(version.getMinimumSupported());
        }
    }
 
    public List<WorkflowTypeComponentImplementationVersion> getWorkflowImplementationComponentVersions() {
        List<WorkflowTypeComponentImplementationVersion> result = new ArrayList<WorkflowTypeComponentImplementationVersion>();
        for (ComponentVersion version : versions.values()) {
            String componentName = version.getComponentName();
            int minimumSupportedImplementationVersion = version.getMaximumSupportedImplementationVersion();
            int maximumSupportedImplementationVersion = version.getMaximumSupportedImplementationVersion();
            int maximumAllowedImplementationVersion = version.getMaximumAllowedImplementationVersion();
            WorkflowTypeComponentImplementationVersion iv = new WorkflowTypeComponentImplementationVersion(componentName,
                    minimumSupportedImplementationVersion, maximumSupportedImplementationVersion,
                    maximumAllowedImplementationVersion);
            result.add(iv);
        }
        return result;
    }
 
}