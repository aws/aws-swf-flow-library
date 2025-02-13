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
import com.amazonaws.services.simpleworkflow.flow.worker.BackoffThrottlerWithJitter;
import com.amazonaws.services.simpleworkflow.flow.worker.ExponentialRetryParameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.retry.RetryUtils;

/**
 * A retrier only for throttling exception.
 * This class is for internal use only and may be changed or removed without prior notice.
 */
public class ThrottlingRetrier extends Retrier {

    private static final Log logger = LogFactory.getLog(ThrottlingRetrier.class);

    public ThrottlingRetrier(ExponentialRetryParameters retryParameters) {
        super(retryParameters, logger);
    }

    @Override
    protected BackoffThrottler createBackoffThrottler() {
        return new BackoffThrottlerWithJitter(getRetryParameters().getInitialInterval(),
                getRetryParameters().getMaximumRetryInterval(), getRetryParameters().getBackoffCoefficient());
    }

    @Override
    protected boolean shouldRetry(RuntimeException e) {
        return e instanceof AwsServiceException && RetryUtils.isThrottlingException((AwsServiceException) e);
    }
}
