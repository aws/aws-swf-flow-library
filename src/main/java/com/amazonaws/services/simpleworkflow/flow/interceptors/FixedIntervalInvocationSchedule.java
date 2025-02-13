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
package com.amazonaws.services.simpleworkflow.flow.interceptors;

import java.util.Date;

import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;

/**
 * Schedule that represents invocations with a fixed delay up to a specified expiration interval or count.
 * 
 * @author fateev
 */
public class FixedIntervalInvocationSchedule implements InvocationSchedule {

    protected static final int SECOND = 1000;

    private final long intervalMilliseconds;
    private final long expirationMilliseconds;
    private final int maxInvocationCount;

    public FixedIntervalInvocationSchedule(int intervalSeconds, int expirationSeconds, int maxInvocationCount) {
        this.intervalMilliseconds = intervalSeconds * SECOND;
        this.expirationMilliseconds = expirationSeconds * SECOND;
        this.maxInvocationCount = maxInvocationCount;
    }
    
    public FixedIntervalInvocationSchedule(int intervalSeconds, int expirationSeconds) {
        this.intervalMilliseconds = intervalSeconds * SECOND;
        this.expirationMilliseconds = expirationSeconds * SECOND;
        this.maxInvocationCount = Integer.MAX_VALUE;
    }

    @Override
    public long nextInvocationDelaySeconds(Date currentTime, Date startTime, Date lastInvocationTime, int pastInvocatonsCount) {
        if (pastInvocatonsCount >= maxInvocationCount) {
            return FlowConstants.NONE;
        }
        long resultMilliseconds;
        if (lastInvocationTime == null) {
            resultMilliseconds = startTime.getTime() + intervalMilliseconds - currentTime.getTime();
        }
        else {
            resultMilliseconds = lastInvocationTime.getTime() + intervalMilliseconds - currentTime.getTime();
        }
        if (resultMilliseconds < 0) {
            resultMilliseconds = 0;
        }
        if (currentTime.getTime() + resultMilliseconds - startTime.getTime() >= expirationMilliseconds) {
            return FlowConstants.NONE;
        }
        return resultMilliseconds / SECOND;
    }

}
