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
package com.amazonaws.services.simpleworkflow.flow.worker;

import com.amazonaws.services.simpleworkflow.flow.annotations.ActivityCompletionRetryOptions;

/**
 * See {@link ActivityCompletionRetryOptions} for default values.
 */
public class ActivityTypeCompletionRetryOptions {

    @ActivityCompletionRetryOptions
    public static final void dummy() {
    }

    private long initialRetryIntervalSeconds;

    private long maximumRetryIntervalSeconds;

    private long retryExpirationSeconds;

    private double backoffCoefficient;

    private int maximumAttempts;

    private int minimumAttempts;

    public ActivityTypeCompletionRetryOptions() {
        try {
            ActivityCompletionRetryOptions defaults = this.getClass().getMethod("dummy").getAnnotation(
                    ActivityCompletionRetryOptions.class);
            initialRetryIntervalSeconds = defaults.initialRetryIntervalSeconds();
            maximumRetryIntervalSeconds = defaults.maximumRetryIntervalSeconds();
            retryExpirationSeconds = defaults.retryExpirationSeconds();
            backoffCoefficient = defaults.backoffCoefficient();
            maximumAttempts = defaults.maximumAttempts();
            minimumAttempts = defaults.minimumAttempts();
        }
        catch (Exception e) {
            throw new Error("Unexpected", e);
        }
    }

    public long getInitialRetryIntervalSeconds() {
        return initialRetryIntervalSeconds;
    }

    public void setInitialRetryIntervalSeconds(long initialRetryIntervalSeconds) {
        this.initialRetryIntervalSeconds = initialRetryIntervalSeconds;
    }

    public long getMaximumRetryIntervalSeconds() {
        return maximumRetryIntervalSeconds;
    }

    public void setMaximumRetryIntervalSeconds(long maximumRetryIntervalSeconds) {
        this.maximumRetryIntervalSeconds = maximumRetryIntervalSeconds;
    }

    public long getRetryExpirationSeconds() {
        return retryExpirationSeconds;
    }

    public void setRetryExpirationSeconds(long retryExpirationSeconds) {
        this.retryExpirationSeconds = retryExpirationSeconds;
    }

    public double getBackoffCoefficient() {
        return backoffCoefficient;
    }

    public void setBackoffCoefficient(double backoffCoefficient) {
        this.backoffCoefficient = backoffCoefficient;
    }

    public int getMaximumAttempts() {
        return maximumAttempts;
    }

    public void setMaximumAttempts(int maximumAttempts) {
        this.maximumAttempts = maximumAttempts;
    }

    public int getMinimumAttempts() {
        return minimumAttempts;
    }

    public void setMinimumAttempts(int minimumAttempts) {
        this.minimumAttempts = minimumAttempts;
    }

}
