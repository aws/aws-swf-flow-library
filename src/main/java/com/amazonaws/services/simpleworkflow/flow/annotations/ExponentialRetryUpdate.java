/**
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow.annotations;
 
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
 
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
 
/**
 * Used to update values of &#064;{@link ExponentialRetry} without requiring
 * workflow type version change. Any changes to &#064;ExponentialRetry might
 * break determinism of a workflow execution. The standard mechanism for non
 * backward compatible changes to workflow definition without workflow type
 * version change is to use "implementation version" documented at
 * {@link WorkflowContext#isImplementationVersion(String, int)}.
 * &#064;ExponentialRetryUpdate contains a list of &#064;ExponentialRetry
 * annotations with associated implementation versions. An interceptor that
 * implements &#064;ExponentialRetry internally calls
 * {@link WorkflowContext#isImplementationVersion(String, int)} for each
 * version.
 * 
 * @author fateev
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ExponentialRetryUpdate {
 
    String component();
 
    ExponentialRetryVersion[] versions();
 
}