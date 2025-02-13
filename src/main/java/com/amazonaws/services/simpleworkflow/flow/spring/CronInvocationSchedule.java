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
package com.amazonaws.services.simpleworkflow.flow.spring;

import java.util.Date;
import java.util.TimeZone;

import org.springframework.scheduling.support.CronSequenceGenerator;

import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.interceptors.InvocationSchedule;

public class CronInvocationSchedule implements InvocationSchedule {

    protected static final int SECOND = 1000;

    private final CronSequenceGenerator cronSequenceGenerator;

    private final Date expiration;

    public CronInvocationSchedule(String cronExpression, Date expiration, TimeZone timeZone) {
        cronSequenceGenerator = new CronSequenceGenerator(cronExpression, timeZone);
        this.expiration = expiration;
    }

    @Override
    public long nextInvocationDelaySeconds(Date currentTime, Date startTime, Date lastInvocationTime, int pastInvocatonsCount) {
        Date nextInvocationTime;
        if (lastInvocationTime == null) {
            nextInvocationTime = cronSequenceGenerator.next(startTime);
        }
        else {
            nextInvocationTime = cronSequenceGenerator.next(lastInvocationTime);
        }
        long resultMilliseconds = nextInvocationTime.getTime() - currentTime.getTime();
        if (resultMilliseconds < 0) {
            resultMilliseconds = 0;
        }
        if (currentTime.getTime() + resultMilliseconds >= expiration.getTime()) {
            return FlowConstants.NONE;
        }
        return resultMilliseconds / SECOND;
    }

}
