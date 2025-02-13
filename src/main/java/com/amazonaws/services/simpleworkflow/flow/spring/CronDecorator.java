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

import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.interceptors.ScheduleDecorator;

/**
 * Repeats any call to the decorated object according to a schedule specified using unix "cron" syntax.
 * Relies on {@link CronSequenceGenerator} for cron parsing and interpretation.
 * 
 * @author fateev
 */
public class CronDecorator extends ScheduleDecorator {

    public CronDecorator(String cronExpression, Date expiration, TimeZone timeZone, WorkflowClock clock) {
        super(new CronInvocationSchedule(cronExpression, expiration, timeZone), clock);
    }
    
}
