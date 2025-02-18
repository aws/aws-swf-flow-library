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
import org.apache.commons.logging.LogFactory;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 *
 */
public class SynchronousRetrier extends Retrier {

    private static final Log logger = LogFactory.getLog(SynchronousRetrier.class);

    private final Class<?>[] exceptionsToNotRetry;

    public SynchronousRetrier(ExponentialRetryParameters retryParameters, Class<?>... exceptionsToNotRetry) {
        super(retryParameters, logger);
        this.exceptionsToNotRetry = exceptionsToNotRetry;
    }
    
    public Class<?>[] getExceptionsToNotRetry() {
        return exceptionsToNotRetry;
    }

    @Override
    protected BackoffThrottler createBackoffThrottler() {
        return new BackoffThrottler(getRetryParameters().getInitialInterval(),
                getRetryParameters().getMaximumRetryInterval(), getRetryParameters().getBackoffCoefficient());
    }

    @Override
    protected boolean shouldRetry(RuntimeException e) {
        for (Class<?> exceptionToNotRetry : getExceptionsToNotRetry()) {
            if (exceptionToNotRetry.isAssignableFrom(e.getClass())) {
                return false;
            }
        }

        return true;
    }
}
