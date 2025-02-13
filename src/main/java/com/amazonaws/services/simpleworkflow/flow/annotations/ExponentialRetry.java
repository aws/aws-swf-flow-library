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

import com.amazonaws.services.simpleworkflow.flow.common.FlowDefaults;
import com.amazonaws.services.simpleworkflow.flow.interceptors.RetryDecorator;

/**
 * This annotation can be used for retrying failures on any asynchronous executions.
 * <p>
 * For retrying based on dynamic retry policy use {@link RetryDecorator}.
 * Both @ExponentialRetry annotation and {@link RetryDecorator} should not be
 * used simultaneously on the same asynchronous method call.
 * <p>
 * To allow retries for an individual activity, place the annotation on the desired 
 * activity method in the corresponding @Activities <b>interface</b>.  Since a new
 * activity instance is started per retry, any timeouts configured on the 
 * {@literal @}ActivityRegistrationOptions apply to each individual retry of the activity.
 * <p>
 * To allow retries for an entire workflow, place the annotation on the override
 * of the workflow's @Execute method in the @Workflow's <b>implementation</b>
 * (rather than on the interface).  Since the same workflow instance is reused 
 * for each retry, any timeouts configured on the @WorkflowRegistrationOptions 
 * apply to the total time of the workflow over all retries.
 * 
 * @author fateev, samar
 *
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExponentialRetry {

    /**
     * Interval to wait after the initial failure, before triggering a retry.
     * <p>
     * This value should not be greater than values specified for 
     * maximumRetryPeriod or retryExpirationPeriod.
     *
     * @return initial retry interval in seconds
     */
    long initialRetryIntervalSeconds();

    /**
     * Maximum interval to wait between retry attempts.
     * <p>
     * This value should not be less than value specified for
     * initialRetryPeriod. Default value is unlimited.
     *
     * @return maximum retry interval in seconds
     */
    long maximumRetryIntervalSeconds() default FlowDefaults.EXPONENTIAL_RETRY_MAXIMUM_RETRY_INTERVAL_SECONDS;

    /**
     * Total duration across all attempts before giving up and attempting
     * no further retries.
     * <p>
     * This duration is measured relative to the initial attempt's starting time.
     * and
     * <p>
     * This value should not be less than value specified for 
     * initialRetryPeriod. Default value is unlimited.
     *
     * @return retry expiration time in seconds
     */
    long retryExpirationSeconds() default FlowDefaults.EXPONENTIAL_RETRY_RETRY_EXPIRATION_SECONDS;

    /**
     * Coefficient to use for exponential retry policy.
     * <p>
     * The retry interval will be multiplied by this coefficient after each
     * subsequent failure.  Default is 2.0.
     *
     * @return backoff multiplier coefficient
     */
    double backoffCoefficient() default FlowDefaults.EXPONENTIAL_RETRY_BACKOFF_COEFFICIENT;

    /**
     * Number of maximum retry attempts (including the initial attempt).
     * Default value is no limit.
     *
     * @return maximum number of retry attempts
     */
    int maximumAttempts() default FlowDefaults.EXPONENTIAL_RETRY_MAXIMUM_ATTEMPTS;

    /**
     * Default is {@link Throwable} which means that all exceptions are retried.
     *
     * @return array of exception classes to retry
     */
    Class<? extends Throwable>[] exceptionsToRetry() default { Throwable.class };

    /**
     * What exceptions that match exceptionsToRetry list should be not retried.
     * Default is empty list.
     *
     * @return array of exception classes to exclude
     */
    Class<? extends Throwable>[] excludeExceptions() default {};

}
