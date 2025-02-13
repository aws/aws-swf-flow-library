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
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.DescribeWorkflowExecutionRequest;

/**
 * Indicates that method is used to retrieve current workflow state. The method
 * is expected to perform read only access of the workflow implementation object
 * and is invoked synchronously which disallows use of any asynchronous
 * operations (like calling methods annotated with {@link Asynchronous}).
 * 
 * Method is expected to have empty list of parameters.
 * {@link Promise} or <code>void</code> return types are not allowed for the annotated method.
 * 
 * The generated external client implementation uses {@link SwfClient#describeWorkflowExecution(DescribeWorkflowExecutionRequest)}
 * visibility API to retrieve the state. It allows access to the sate using external client if decider 
 * workers are down and even after workflow execution completion.
 * 
 * @author fateev, samar
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GetState {
}
