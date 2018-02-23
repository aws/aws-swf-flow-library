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
package com.amazonaws.services.simpleworkflow.flow.interceptors;

import java.util.Random;
import java.util.Date;

import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;

/**
* Retry policy that adds to the functionality of the ExponentialRetryPolicy by allowing a jitter value
* to the nextRetryDelaySeconds output. The jitter is the value of baseDelay multiplied with a random
* coefficient between 0 and maxJitterCoefficient. And the jitter could randomly be positive or negative:
* <p>
* -(baseDelay * maxJitterCoefficient) <= jitter < (baseDelay * maxJitterCoefficient)
* </p>
* Since maxJitterCoefficient is exclusively between 0 and 1, the absolute value of jitter is always
* smaller than the baseDelay.
* 
* @author congwan
* 
*/
public class ExponentialRetryWithJitterPolicy extends ExponentialRetryPolicy {

    private final Random jitterCoefficientGenerator;

    private final double maxJitterCoefficient;

    /**
     * Makes a new ExponentialRetryWithJitterPolicy object with the specified jitter conditions.
     * @param initialRetryIntervalSeconds the initial interval for retry delays in seconds.
     * @param jitterCoefficientGenerator a random number generator that generates the value for the jitter coefficient.
     * @param maxJitterCoefficient the maximum allowed jitter to be added to the output of the nextRetryDelaySeconds.
     */
    public ExponentialRetryWithJitterPolicy(final long initialRetryIntervalSeconds, final Random jitterCoefficientGenerator, final double maxJitterCoefficient) {
        super(initialRetryIntervalSeconds);
        this.jitterCoefficientGenerator = jitterCoefficientGenerator;
        this.maxJitterCoefficient = maxJitterCoefficient;
    }

    @Override
    public long nextRetryDelaySeconds(final Date firstAttempt, final Date recordedFailure, final int numberOfTries) {
        final long baseDelay = super.nextRetryDelaySeconds(firstAttempt, recordedFailure, numberOfTries);

        long totalDelay;

        if (baseDelay == FlowConstants.NONE) {
            totalDelay = baseDelay;
        } else {
            long jitter = Math.round(baseDelay * maxJitterCoefficient * (2 * jitterCoefficientGenerator.nextDouble() - 1));
            totalDelay = baseDelay + jitter;
        }

        return totalDelay;
    }

    @Override
    public void validate() throws IllegalStateException {
        super.validate();

        if (jitterCoefficientGenerator == null) {
            throw new IllegalStateException("ExponentialRetryWithJitterPolicy requires jitterCoefficientGenerator not to be null.");
        }

        if (maxJitterCoefficient <= 0 || maxJitterCoefficient >= 1) {
            throw new IllegalStateException("ExponentialRetryWithJitterPolicy requires maxJitterCoefficient to be exclusively between 0 and 1.");
        }

    }
	

}
