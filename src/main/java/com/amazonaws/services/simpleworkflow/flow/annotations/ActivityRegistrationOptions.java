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
package com.amazonaws.services.simpleworkflow.flow.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.amazonaws.services.simpleworkflow.flow.ActivitySchedulingOptions;
import com.amazonaws.services.simpleworkflow.flow.ActivityWorker;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;

/**
 * {@literal @}ActivityRegistrationOptions is a required annotation, unless
 * {@link SkipTypeRegistration} is provided, on either interface annotated with 
 * {@link Activities} or activity method.
 * 
 * It contains all the registration options for ActivityType which will be used 
 * for registration with Amazon SWF Service.  Registration of activity types happen
 * on {@link ActivityWorker#start()}.
 * 
 * @see ActivityWorker
 * @author fateev, samar
 * 
 */
@Target({ ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface ActivityRegistrationOptions {

    String description() default "";

    long defaultTaskScheduleToStartTimeoutSeconds();

    /**
     * Default is {@link FlowConstants#NONE} which disables separate heartbeat
     * timeout.
     *
     * @return timeout in seconds, defaults to NONE
     */
    long defaultTaskHeartbeatTimeoutSeconds() default FlowConstants.NONE;

    /**
     * Default is {@link FlowConstants#NONE}.
     *
     * @return timeout in seconds, defaults to NONE
     */
    long defaultTaskScheduleToCloseTimeoutSeconds() default FlowConstants.NONE;

    long defaultTaskStartToCloseTimeoutSeconds();

    /**
     * Task list that activity task is delivered through when no task list is
     * specified on activity invocation.
     * 
     * <p>
     * Default is {@link FlowConstants#USE_WORKER_TASK_LIST}, which means to use task
     * list from the {@link ActivityWorker} that the activity implementation is
     * registered with. Specify {@link FlowConstants#NO_DEFAULT_TASK_LIST} to
     * not register any default task list. If no default task list registered it
     * becomes required scheduling option (specified through
     * {@link ActivitySchedulingOptions#setTaskList(String)}) when an activity
     * is called.
     *
     * @return task list name
     */
    String defaultTaskList() default FlowConstants.USE_WORKER_TASK_LIST;

    /**
     * Default is {@link FlowConstants#DEFAULT_TASK_PRIORITY} if it
     * is not specified on activity invocation
     *
     * @return task priority, defaults to DEFAULT_TASK_PRIORITY
     */
    int defaultTaskPriority() default FlowConstants.DEFAULT_TASK_PRIORITY;
    
}
