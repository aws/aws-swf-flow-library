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
package com.amazonaws.services.simpleworkflow.flow.interceptors;

import java.util.Collection;
import java.util.Date;

import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.common.FlowDefaults;

public class ExponentialRetryPolicy extends RetryPolicyBase {

    private final long initialRetryIntervalSeconds;

    private long maximumRetryIntervalSeconds = FlowDefaults.EXPONENTIAL_RETRY_MAXIMUM_RETRY_INTERVAL_SECONDS;

    private long retryExpirationIntervalSeconds = FlowDefaults.EXPONENTIAL_RETRY_RETRY_EXPIRATION_SECONDS;

    private double backoffCoefficient = FlowDefaults.EXPONENTIAL_RETRY_BACKOFF_COEFFICIENT;

    private int maximumAttempts = FlowDefaults.EXPONENTIAL_RETRY_MAXIMUM_ATTEMPTS;

    public ExponentialRetryPolicy(long initialRetryIntervalSeconds) {
        this.initialRetryIntervalSeconds = initialRetryIntervalSeconds;
    }

    public long getInitialRetryIntervalSeconds() {
        return initialRetryIntervalSeconds;
    }

    public long getMaximumRetryIntervalSeconds() {
        return maximumRetryIntervalSeconds;
    }

    /**
     * Set the upper limit of retry interval. No limit by default.
     * @param maximumRetryIntervalSeconds max retry interval seconds
     */
    public void setMaximumRetryIntervalSeconds(long maximumRetryIntervalSeconds) {
        this.maximumRetryIntervalSeconds = maximumRetryIntervalSeconds;
    }

    public ExponentialRetryPolicy withMaximumRetryIntervalSeconds(long maximumRetryIntervalSeconds) {
        this.maximumRetryIntervalSeconds = maximumRetryIntervalSeconds;
        return this;
    }

    public long getRetryExpirationIntervalSeconds() {
        return retryExpirationIntervalSeconds;
    }

    /**
     * Stop retrying after the specified interval.
     * @param retryExpirationIntervalSeconds retry expiration interval seconds
     */
    public void setRetryExpirationIntervalSeconds(long retryExpirationIntervalSeconds) {
        this.retryExpirationIntervalSeconds = retryExpirationIntervalSeconds;
    }

    public ExponentialRetryPolicy withRetryExpirationIntervalSeconds(long retryExpirationIntervalSeconds) {
        this.retryExpirationIntervalSeconds = retryExpirationIntervalSeconds;
        return this;
    }

    public double getBackoffCoefficient() {
        return backoffCoefficient;
    }

    /**
     * Coefficient used to calculate the next retry interval. The following
     * formula is used:
     * <code>initialRetryIntervalSeconds * Math.pow(backoffCoefficient, numberOfTries - 2)</code>
     * @param backoffCoefficient Multiplier for retry delay
     */
    public void setBackoffCoefficient(double backoffCoefficient) {
        this.backoffCoefficient = backoffCoefficient;
    }

    public ExponentialRetryPolicy withBackoffCoefficient(double backoffCoefficient) {
        this.backoffCoefficient = backoffCoefficient;
        return this;
    }

    public int getMaximumAttempts() {
        return maximumAttempts;
    }

    /**
     * Maximum number of attempts. The first retry is second attempt.
     * @param maximumAttempts Total allowed retry attempts
     */
    public void setMaximumAttempts(int maximumAttempts) {
        this.maximumAttempts = maximumAttempts;
    }

    public ExponentialRetryPolicy withMaximumAttempts(int maximumAttempts) {
        this.maximumAttempts = maximumAttempts;
        return this;
    }

    /**
     * The exception types that cause operation being retried. Subclasses of the
     * specified types are also included. Default is Throwable.class which means
     * retry any exceptions.
     */
    @Override
    public ExponentialRetryPolicy withExceptionsToRetry(Collection<Class<? extends Throwable>> exceptionsToRetry) {
        super.withExceptionsToRetry(exceptionsToRetry);
        return this;
    }

    /**
     * The exception types that should not be retried. Subclasses of the
     * specified types are also not retried. Default is empty list.
     */
    @Override
    public ExponentialRetryPolicy withExceptionsToExclude(Collection<Class<? extends Throwable>> exceptionsToRetry) {
        super.withExceptionsToExclude(exceptionsToRetry);
        return this;
    }

    @Override
    public long nextRetryDelaySeconds(Date firstAttempt, Date recordedFailure, int numberOfTries) {

        if (numberOfTries < 2) {
            throw new IllegalArgumentException("attempt is less then 2: " + numberOfTries);
        }

        if (maximumAttempts > FlowConstants.NONE && numberOfTries > maximumAttempts) {
            return FlowConstants.NONE;
        }

        long result = (long) (initialRetryIntervalSeconds * Math.pow(backoffCoefficient, numberOfTries - 2));
        result = maximumRetryIntervalSeconds > FlowConstants.NONE ? Math.min(result, maximumRetryIntervalSeconds) : result;
        int secondsSinceFirstAttempt = (int) ((recordedFailure.getTime() - firstAttempt.getTime()) / 1000);
        if (retryExpirationIntervalSeconds > FlowConstants.NONE
                && secondsSinceFirstAttempt + result >= retryExpirationIntervalSeconds) {
            return FlowConstants.NONE;
        }

        return result;
    }

    /**
     * Performs the following three validation checks for ExponentialRetry
     * Policy: 1) initialRetryIntervalSeconds is not greater than
     * maximumRetryIntervalSeconds 2) initialRetryIntervalSeconds is not greater
     * than retryExpirationIntervalSeconds
     */
    public void validate() throws IllegalStateException {
        if (maximumRetryIntervalSeconds > FlowConstants.NONE && initialRetryIntervalSeconds > maximumRetryIntervalSeconds) {
            throw new IllegalStateException(
                    "ExponentialRetryPolicy requires maximumRetryIntervalSeconds to have a value larger than initialRetryIntervalSeconds.");
        }

        if (retryExpirationIntervalSeconds > FlowConstants.NONE && initialRetryIntervalSeconds > retryExpirationIntervalSeconds) {
            throw new IllegalStateException(
                    "ExponentialRetryPolicy requires retryExpirationIntervalSeconds to have a value larger than initialRetryIntervalSeconds.");
        }
    }
}
