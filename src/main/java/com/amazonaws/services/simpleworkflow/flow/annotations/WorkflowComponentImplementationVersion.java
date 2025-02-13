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
 
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.WorkflowWorker;
 
/**
 * Specifies supported implementation versions of a component used to implement
 * workflow definition. See
 * {@link WorkflowContext#isImplementationVersion(String, int)} for
 * implementation version overview.
 * <p>
 * To be used as a parameter to {@link WorkflowComponentImplementationVersions}.
 * 
 * @author fateev
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface WorkflowComponentImplementationVersion {
 
    /**
     * Name of a versioned component.
     *
     * @return component name
     */
    String componentName();
 
    /**
     * Minimum code implementation version supported by the workflow definition
     * code. Attempt to replay history that was created with a lower
     * implementation version must fail the decision.
     *
     * @return minimum version number, defaults to 0
     */
    int minimumSupported() default 0;
 
    /**
     * Maximum code implementation version supported by the workflow definition
     * code. Attempt to replay history that was created with a higher
     * implementation version must fail the decision.
     *
     * @return maximum supported version number
     */
    int maximumSupported();
 
    /**
     * Maximum version that newly executed code path can support. This value is
     * usually set to a maximumSupported version of the code that is being
     * upgraded from. After new workflow code is deployed to every worker this
     * value is changed to the maximumSupported value of the new code.
     * <p>
     * To avoid code change after the deployment consider changing maximum
     * allowed implementation version through
     * {@link WorkflowWorker#setMaximumAllowedComponentImplementationVersions(java.util.Map)}.
     *
     * @return maximum allowed version, defaults to Integer.MAX_VALUE
     */
    int maximumAllowed() default Integer.MAX_VALUE;
 
}