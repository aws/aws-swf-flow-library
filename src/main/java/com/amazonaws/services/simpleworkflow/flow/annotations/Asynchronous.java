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

import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.TryCatchFinally;

/**
 * Call to @Asynchronous method always returns immediately without executing it
 * code. Method body is scheduled for execution after all parameters that extend
 * {@link Promise} and not marked with {@link NoWait} are ready. The only valid
 * return types for @Asynchronous method are <code>void</code> and
 * {@link Promise}.
 * 
 * @author fateev
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Asynchronous {

    /**
     * if set to true treats asynchronous task as a daemon task, allowing the
     * parent asynchronous scope to close if all non-daemon child tasks
     * completes. Default is <code>false</code> which means use the value of the
     * parent task. See {@link TryCatchFinally} for more info on daemon
     * semantic.
     *
     * @return true if task should be treated as daemon
     */
    boolean daemon() default false;
}
