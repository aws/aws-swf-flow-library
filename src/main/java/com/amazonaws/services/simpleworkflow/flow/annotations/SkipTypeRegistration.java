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

import com.amazonaws.services.simpleworkflow.flow.ActivityWorker;
import com.amazonaws.services.simpleworkflow.flow.WorkflowWorker;

/**
 * This can be used on interfaces annotated with {@link Activities} or {@link Workflow}
 * to specify no registration options are needed for ActivityType or WorkflowType
 * defined by such interfaces.
 * 
 * Registration of types is skipped by {@link ActivityWorker} or {@link WorkflowWorker}
 * when interface is annotated with @SkipTypeRegistration.
 * 
 * @see Activities
 * @see Workflow
 * @see ActivityWorker
 * @see WorkflowWorker
 * @author fateev, samar
 *
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface SkipTypeRegistration {

}
