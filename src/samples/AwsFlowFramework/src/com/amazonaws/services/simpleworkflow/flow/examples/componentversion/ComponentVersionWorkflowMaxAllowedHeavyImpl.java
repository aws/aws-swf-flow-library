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
package com.amazonaws.services.simpleworkflow.flow.examples.componentversion;

import com.amazonaws.services.simpleworkflow.flow.DecisionContextProviderImpl;
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.annotations.WorkflowComponentImplementationVersion;
import com.amazonaws.services.simpleworkflow.flow.annotations.WorkflowComponentImplementationVersions;

@WorkflowComponentImplementationVersions(value = {
        @WorkflowComponentImplementationVersion(componentName = "HEAVY_LOAD_COMPONENT", minimumSupported = 0, maximumSupported = 1) })
public class ComponentVersionWorkflowMaxAllowedHeavyImpl extends ComponentVersionWorkflowImplBase {

    private WorkflowContext context = new DecisionContextProviderImpl().getDecisionContext().getWorkflowContext();

    @Override
    public void runComponentVersionWorkflow(String input) {
        if (context.isImplementationVersion("HEAVY_LOAD_COMPONENT", 1)) {
            doHeavyLoadWork();
        } else {
            doLightLoadWork();
        }

    }
}
