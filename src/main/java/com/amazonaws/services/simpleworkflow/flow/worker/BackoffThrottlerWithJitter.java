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

import java.util.Random;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 *
 */
public class BackoffThrottlerWithJitter extends BackoffThrottler {

    private final Random rand = new Random();

    public BackoffThrottlerWithJitter(long initialSleep, long maxSleep, double backoffCoefficient) {
        super(initialSleep, maxSleep, backoffCoefficient);
    }

    @Override
    protected long calculateSleepTime() {
        int delay = (int) (2 * initialSleep * Math.pow(backoffCoefficient, failureCount.get() - 1));
        return Math.min(2 * maxSleep, rand.nextInt(delay));
    }
}
