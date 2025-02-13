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

import com.amazonaws.services.simpleworkflow.flow.WorkflowWorker;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;

/**
 * {@literal @}Execute annotation is used on methods of interface annotated with {@link Workflow}
 * to specify the entry-point for WorkflowType.  
 * 
 * {@literal @}Execute method can only have <code>void</code> or {@link Promise} return types.
 * Parameters of type {@link Promise} are not allowed.
 * 
 * @see Workflow
 * @see WorkflowWorker
 * @author fateev, samar
 * 
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Execute {

    /**
     * Optional name of the workflow type. When missing defaults to the
     * annotated method name. Maximum length is 256 characters.
     *
     * Don't set this name for a parent interface if you want to inherit its @Execute annotated method.
     *
     * @return workflow type name
     */
    String name() default "";

    /**
     * Required version of the workflow type. Maximum length is 64 characters.
     *
     * @return workflow type version
     */
    String version();

}
