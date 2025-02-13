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
package com.amazonaws.services.simpleworkflow.flow.retry;

import com.amazonaws.services.simpleworkflow.flow.worker.BackoffThrottler;
import com.amazonaws.services.simpleworkflow.flow.worker.ExponentialRetryParameters;
import org.apache.commons.logging.Log;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 *
 */
public abstract class Retrier {

    private final ExponentialRetryParameters retryParameters;
    private final Log logger;

    public Retrier(ExponentialRetryParameters retryParameters, Log logger) {
        if (retryParameters.getBackoffCoefficient() < 0) {
            throw new IllegalArgumentException("negative backoffCoefficient");
        }
        if (retryParameters.getInitialInterval() < 10) {
            throw new IllegalArgumentException("initialInterval cannot be less then 10: " + retryParameters.getInitialInterval());
        }
        if (retryParameters.getExpirationInterval() < retryParameters.getInitialInterval()) {
            throw new IllegalArgumentException("expirationInterval < initialInterval");
        }
        if (retryParameters.getMaximumRetries() < retryParameters.getMinimumRetries()) {
            throw new IllegalArgumentException("maximumRetries < minimumRetries");
        }
        this.retryParameters = retryParameters;
        this.logger = logger;
    }

    protected abstract BackoffThrottler createBackoffThrottler();

    protected abstract boolean shouldRetry(RuntimeException e);

    public void retry(Runnable r) {
        int attempt = 0;
        long startTime = System.currentTimeMillis();
        BackoffThrottler throttler = createBackoffThrottler();
        boolean success = false;
        do {
            try {
                attempt++;
                throttler.throttle();
                r.run();
                success = true;
                throttler.success();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            catch (RuntimeException e) {
                throttler.failure();
                if (!shouldRetry(e)) {
                    throw e;
                }
                long elapsed = System.currentTimeMillis() - startTime;
                if (attempt > retryParameters.getMaximumRetries()
                        || (elapsed >= retryParameters.getExpirationInterval() && attempt > retryParameters.getMinimumRetries())) {
                    throw e;
                }
                logger.warn("Retrying after failure", e);
            }
        }
        while (!success);
    }

    public ExponentialRetryParameters getRetryParameters() {
        return retryParameters;
    }
}
