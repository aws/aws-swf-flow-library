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

/**
 * Used to mark {@link Promise} arguments of @Asynchronous methods that should
 * not be waited for.
 * <p>
 * Example usage:
 * 
 * <pre>
 * <code>
 * {@literal @}Asynchronous
 * private void calculate(Promise&lt;Integer&gt; arg1, @NoWait Settable&lt;Integer&gt; result) {
 *    ...
 *    result.set(r);
 * }
 * </code>
 * </pre>
 * 
 * @see Asynchronous
 * 
 * @author fateev
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface NoWait {

}
