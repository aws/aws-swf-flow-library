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

import com.amazonaws.services.simpleworkflow.flow.core.Promise;

public abstract class ComponentVersionWorkflowImplBase implements ComponentVersionWorkflow {

    protected ComponentVersionActivitiesClient activityClient = new ComponentVersionActivitiesClientImpl();

    protected void doLightLoadWork() {
        Promise<Void> waitFor = activityClient.doSomeWork();
        activityClient.doMoreWork(waitFor);
    }

    protected void doHeavyLoadWork() {
        Promise<Void> waitFor = activityClient.doMoreWork();
        activityClient.doALotOfWork(waitFor);
    }
}
