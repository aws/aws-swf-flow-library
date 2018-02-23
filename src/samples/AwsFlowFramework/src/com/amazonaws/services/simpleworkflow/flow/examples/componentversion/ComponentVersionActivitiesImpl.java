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

public class ComponentVersionActivitiesImpl implements ComponentVersionActivities {

    @Override
    public void doSomeWork() {
        System.out.println("Start doing some work.");
        doWork(5);
        System.out.println("Some work finished.");
    }

    @Override
    public void doMoreWork() {
        System.out.println("Start doing more work.");
        doWork(10);
        System.out.println("More work finished.");
    }

    @Override
    public void doALotOfWork() {
        System.out.println("Start doing a lot of work.");
        doWork(15);
        System.out.println("A lot of work finished.");
    }

    private void doWork(int x) {
        try {
            Thread.sleep(x * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
