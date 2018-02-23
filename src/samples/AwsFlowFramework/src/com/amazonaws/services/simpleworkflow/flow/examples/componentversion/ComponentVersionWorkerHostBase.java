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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.simpleworkflow.flow.ActivityWorker;
import com.amazonaws.services.simpleworkflow.flow.WorkflowWorker;
import com.amazonaws.services.simpleworkflow.flow.examples.common.ConfigHelper;

public abstract class ComponentVersionWorkerHostBase {

    private static final String ACTIVITIES_TASK_LIST = "ComponentVersionActivityTaskList";

    private static final String DECISION_TASK_LIST = "ComponentVersionDecisionTaskList";

    private ConfigHelper configHelper;

    public ComponentVersionWorkerHostBase() throws IllegalArgumentException, IOException {
        configHelper = ConfigHelper.createConfig();
    }

    protected ActivityWorker startActivityWorker()
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        final ActivityWorker activityWorker = new ActivityWorker(configHelper.createSWFClient(),
                configHelper.getDomain(), ACTIVITIES_TASK_LIST);
        ComponentVersionActivities componentVersionActivitiesImpl = new ComponentVersionActivitiesImpl();
        activityWorker.addActivitiesImplementation(componentVersionActivitiesImpl);
        activityWorker.start();
        System.out.println("Started activity worker");
        return activityWorker;
    }

    protected WorkflowWorker startWorkflowWorker(
            Class<? extends ComponentVersionWorkflow> componentVersionWorkflowImplClass)
            throws InstantiationException, IllegalAccessException {
        final WorkflowWorker workflowWorker = new WorkflowWorker(configHelper.createSWFClient(),
                configHelper.getDomain(), DECISION_TASK_LIST);
        workflowWorker.addWorkflowImplementationType(componentVersionWorkflowImplClass);
        workflowWorker.start();
        System.out.println("Started workflow worker: " + componentVersionWorkflowImplClass.getName());
        return workflowWorker;
    }

    protected void addShutDownHook(ActivityWorker activityWorker, WorkflowWorker workflowWorker) {
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    workflowWorker.shutdownAndAwaitTermination(1, TimeUnit.MINUTES);
                    System.out.println("Workflow Host Service Terminated...");
                    activityWorker.shutdownAndAwaitTermination(1, TimeUnit.MINUTES);
                    System.out.println("Activity Worker Exited.");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        System.out.println("Please press any key to terminate service.");

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
