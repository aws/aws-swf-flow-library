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

/**
 * @author fateev
 */
class ComponentVersion {
 
    private final String componentName;
 
    private int maximumSupportedImplementationVersion;
 
    private int minimumSupportedImplementationVersion;
 
    private int maximumAllowedImplementationVersion = Integer.MAX_VALUE;
 
    /**
     * Actual current version
     */
    private int currentVersion;
 
    private Integer versionFromHistory;
 
    private Integer maxSkippedVersion;
 
    public ComponentVersion(String componentName) {
        this.componentName = componentName;
    }
 
    void setVersionFromHistory(int version) {
        if (versionFromHistory != null && version < versionFromHistory) {
            throw new IncompatibleWorkflowDefinition("Version from history cannot decrease from " + versionFromHistory + " to "
                    + version + " for \"" + componentName + "\" component");
        }
        currentVersion = version;
        versionFromHistory = version;
    }
 
    public String getComponentName() {
        return componentName;
    }
 
    public int getMaximumSupportedImplementationVersion() {
        return maximumSupportedImplementationVersion;
    }
 
    public void setMaximumSupportedImplementationVersion(int maximumSupportedImplementationVersion) {
        if (versionFromHistory != null && maximumSupportedImplementationVersion < versionFromHistory) {
            throw new IncompatibleWorkflowDefinition("Maximum supported implementation version="
                    + maximumSupportedImplementationVersion + " is below one found in the history " + versionFromHistory
                    + " for \"" + componentName + "\" component.");
        }
        this.maximumSupportedImplementationVersion = maximumSupportedImplementationVersion;
    }
 
    public int getMinimumSupportedImplementationVersion() {
        return minimumSupportedImplementationVersion;
    }
 
    public void setMinimumSupportedImplementationVersion(int minimumSupportedImplementationVersion) {
        this.minimumSupportedImplementationVersion = minimumSupportedImplementationVersion;
        if (versionFromHistory != null && versionFromHistory < minimumSupportedImplementationVersion) {
            throw new IncompatibleWorkflowDefinition("Minimum supported implementation version="
                    + minimumSupportedImplementationVersion + " is larger then one found in the history " + versionFromHistory
                    + " for \"" + componentName + "\" component.");
        }
        if (maximumAllowedImplementationVersion < minimumSupportedImplementationVersion) {
            throw new IncompatibleWorkflowDefinition("Minimum supported implementation version="
                    + minimumSupportedImplementationVersion + " is larger then maximumAllowedImplementationVersion="
                    + maximumAllowedImplementationVersion + " for \"" + componentName + "\" component.");
        }
        if (minimumSupportedImplementationVersion > currentVersion) {
            currentVersion = minimumSupportedImplementationVersion;
        }
    }
 
    public int getMaximumAllowedImplementationVersion() {
        return maximumAllowedImplementationVersion;
    }
 
    public void setMaximumAllowedImplementationVersion(int maximumAllowedImplementationVersion) {
        this.maximumAllowedImplementationVersion = maximumAllowedImplementationVersion;
    }
 
    public int getCurrentVersion() {
        return currentVersion;
    }
 
    public boolean isVersion(int version, boolean replaying) {
        if (maximumSupportedImplementationVersion < version) {
            throw new IncompatibleWorkflowDefinition("version=" + version
                    + " is larger then maximumSupportedImplementationVersion=" + maximumSupportedImplementationVersion
                    + " for \"" + componentName + "\" component.");
        }
        if (minimumSupportedImplementationVersion > version) {
            throw new IncompatibleWorkflowDefinition("version=" + version
                    + " is smaller then minimumSupportedImplementationVersion=" + minimumSupportedImplementationVersion
                    + " for \"" + componentName + "\" component.");
        }
        if (maxSkippedVersion != null && maxSkippedVersion <= version) {
            return false;
        }
        if (currentVersion >= version) {
            return true;
        }
        else if (replaying) {
            if (version == minimumSupportedImplementationVersion) {
                currentVersion = version;
                return true;
            }
            if (maxSkippedVersion == null || maxSkippedVersion < version) {
                maxSkippedVersion = version;
            }
            return false;
        }
        else {
            if (maximumAllowedImplementationVersion < version) {
                return false;
            }
            currentVersion = version;
            return true;
        }
    }
}