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

import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;

/**
 * This annotation can be used for retrying failures on any asynchronous executions.
 * This is an expansion of the annotation
 * {@link com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetry}
 * 
 * @author congwan
 *
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExponentialRetryWithJitter {
	
    /*
     * Maximum jitter coefficient to apply to retry interval.
     * This is the range for coefficient from which we draw a uniform random
     * number for the variation to be introduced in the delay. Default
     * value is 0.5.
     */
    double maxJitterCoefficient() default FlowConstants.EXPONENTIAL_RETRY_MAXIMUM_JITTER_COEFFICIENT;
    
    /*
     * Interval to wait after the initial failure, before triggering a retry.
     * This value must not be greater than values specified for
     * maximumRetryPeriod or retryExpirationPeriod. Default value is 5.
     */
    long initialRetryIntervalSeconds() default FlowConstants.EXPONENTIAL_INITIAL_RETRY_INTERVAL_SECONDS;

    /*
     * Maximum interval to wait between retry attempts.
     * This value must not be less than value specified for
     * initialRetryPeriod. Default value is unlimited.
     */

    long maximumRetryIntervalSeconds() default FlowConstants.EXPONENTIAL_RETRY_MAXIMUM_RETRY_INTERVAL_SECONDS;

    /*
     * Total duration across all attempts before giving up and attempting no
     * further retries.
     * This duration is measured relative to the initial attempt's starting
     * time. This value must not be less than value specified for
     * initialRetryPeriod. Default value is unlimited.
     */
    long retryExpirationSeconds() default FlowConstants.EXPONENTIAL_RETRY_EXPIRATION_SECONDS;

    /*
     * Coefficient to use for exponential retry policy.
     * The retry interval will be multiplied by this coefficient after each
     * subsequent failure. Default is 2.0.
     * 
     * This value must greater than 1.0. Otherwise the delay would be smaller and smaller.
     */
    double backoffCoefficient() default FlowConstants.EXPONENTIAL_RETRY_BACKOFF_COEFFICIENT;

    /*
     * Number of maximum retry attempts (including the initial attempt). Default
     * value is no limit.
     */
	
    int maximumAttempts() default FlowConstants.EXPONENTIAL_RETRY_MAXIMUM_ATTEMPTS;

    /*
     * Default is {@link Throwable} which means that all exceptions are retried.
     */
    Class<? extends Throwable>[] exceptionsToRetry() default { Throwable.class };

    /*
     * What exceptions that match exceptionsToRetry list must be not retried.
     * Default is empty list.
     */
    Class<? extends Throwable>[] excludeExceptions() default {};

}
