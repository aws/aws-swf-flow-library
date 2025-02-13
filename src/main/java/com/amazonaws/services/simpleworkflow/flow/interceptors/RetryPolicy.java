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

import java.util.Date;

import com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetry;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;

/**
 * Defines retry policy in case of failures. Valid implementation should be
 * stateless and thread safe.
 * 
 * @see RetryDecorator
 * @see ExponentialRetry
 */
public interface RetryPolicy {

    boolean isRetryable(Throwable failure);


    /**
     * Calculates delay for next retry attempt.
     * @param firstAttempt Initial attempt timestamp
     * @param recordedFailure Last failure timestamp
     * @param numberOfTries Current retry count
     * @return Time to the next retry.
     */
    long nextRetryDelaySeconds(Date firstAttempt, Date recordedFailure, int numberOfTries);
}
