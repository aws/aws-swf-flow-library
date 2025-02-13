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

import com.amazonaws.services.simpleworkflow.flow.common.FlowDefaults;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ActivityCompletionRetryOptions {

    /**
     * Interval to wait after the initial failure, before triggering a retry.
     * <p>
     * This value should not be greater than values specified for
     * maximumRetryPeriod or retryExpirationPeriod. Default is 10 seconds.
     *
     * @return initial retry interval in seconds
     */
    long initialRetryIntervalSeconds() default 10;

    /**
     * Maximum interval to wait between retry attempts.
     * <p>
     * This value should not be less than value specified for
     * initialRetryPeriod. Default value is 60 seconds.
     *
     * @return maximum retry interval in seconds
     */
    long maximumRetryIntervalSeconds() default 60;

    /**
     * Total duration across all attempts before giving up and attempting no
     * further retries.
     * <p>
     * This duration is measured relative to the initial attempt's starting
     * time. and
     * <p>
     * This value should not be less than value specified for
     * initialRetryPeriod. Default value is 300.
     *
     * @return retry expiration time in seconds
     */
    long retryExpirationSeconds() default 300;

    /**
     * Coefficient to use for exponential retry policy.
     * <p>
     * The retry interval will be multiplied by this coefficient after each
     * subsequent failure. Default is 2.0.
     *
     * @return backoff multiplier coefficient
     */
    double backoffCoefficient() default FlowDefaults.EXPONENTIAL_RETRY_BACKOFF_COEFFICIENT;

    /**
     * Number of maximum retry attempts (including the initial attempt). Default
     * value is 10.
     *
     * @return maximum number of retry attempts
     */
    int maximumAttempts() default 10;

    /**
     * Minimum number of retry attempts (including the initial attempt). In case
     * of failures at least this number of attempts is executed independently of
     * {@link #retryExpirationSeconds()}. Default value is 1.
     *
     * @return minimum number of retry attempts
     */
    int minimumAttempts() default 1;

}
