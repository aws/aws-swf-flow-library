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
 * Encapsulates an invocation schedule.
 * 
 * @see AsyncScheduledExecutor
 * @see ScheduleDecorator
 * @author fateev
 */
public interface InvocationSchedule {

    /**
     * Return interval until the next invocation.
     * 
     * @param currentTime
     *            - current workflow time
     * @param startTime
     *            - time when workflow started
     * @param lastInvocationTime
     *            - time when last invocation happened
     * @param pastInvocatonsCount
     *            - how many invocations were done
     * @return time in seconds until the next invocation.
     *         {@link FlowConstants#NONE} if no more invocations should be
     *         scheduled.
     */
    long nextInvocationDelaySeconds(Date currentTime, Date startTime, Date lastInvocationTime, int pastInvocatonsCount);

}
