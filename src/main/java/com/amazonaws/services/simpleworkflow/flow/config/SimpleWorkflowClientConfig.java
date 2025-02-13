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
package com.amazonaws.services.simpleworkflow.flow.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.time.Duration;

@Builder
@AllArgsConstructor
public class SimpleWorkflowClientConfig {

    private final Duration pollingRequestTimeout;

    private final Duration dataPlaneRequestTimeout;

    private final Duration controlPlaneRequestTimeout;

    /**
     * Maximum attempts for retrying failed RequestCancelExternalWorkflowExecutionDecision
     */
    @Getter
    private final int maxRetryForCancelingExternalWorkflow;

    /**
     * Time to sleep on the first failure of RequestCancelExternalWorkflowExecutionDecision
     */
    private final Duration initialSleepForDecisionThrottle;

    /**
     * Maximum time to sleep independently of number of failures
     */
    private final Duration maxSleepForDecisionThrottle;

    /**
     * Coefficient used to calculate the next time to sleep
     */
    @Getter
    private final double backoffCoefficientForDecisionThrottle;

    @Getter
    private final DeciderAffinityConfig deciderAffinityConfig;

    private static final int DEFAULT_POLLING_REQUEST_TIMEOUT_IN_SEC = 70;

    private static final int DEFAULT_DATA_PLANE_REQUEST_TIMEOUT_IN_SEC = 6;

    private static final int DEFAULT_CONTROL_PLANE_REQUEST_TIMEOUT_IN_SEC = 10;

    private static final int DEFAULT_MAX_RETRY_FOR_CANCELING_EXTERNAL_WORKFLOW = 0;

    private static final int DEFAULT_INITIAL_SLEEP_FOR_DECISION_THROTTLE_IN_SEC = 1;

    private static final int DEFAULT_MAX_SLEEP_FOR_DECISION_THROTTLE_IN_SEC = 0;

    private static final double DEFAULT_BACKOFF_COEFFICIENT_FOR_DECISION_THROTTLE = 2;

    private static final DeciderAffinityConfig DEFAULT_DECIDER_AFFINITY_CONFIG = null;

    /**
     * Class method to provide SimpleWorkflowClientConfig with default timeouts
     *
     * @return SimpleWorkflowClientConfig instance
     */
    public static SimpleWorkflowClientConfig ofDefaults() {
        return new SimpleWorkflowClientConfigBuilder().build();
    }

    /**
     * Create SimpleWorkflowClientConfig with polling/dataPlane/controlPlane timeouts
     *
     * @param pollingRequestTimeout timeout for polling APIs like PollForActivityTask, PollForDecisionTask
     * @param dataPlaneRequestTimeout timeout for dataPlane APIs like StartWorkflowExecution, RespondActivityTaskCompleted
     * @param controlPlaneRequestTimeout timeout for controlPlane APIs like ListDomains, ListActivityTypes
     */
    public SimpleWorkflowClientConfig(Duration pollingRequestTimeout, Duration dataPlaneRequestTimeout, Duration controlPlaneRequestTimeout) {
        this(pollingRequestTimeout, dataPlaneRequestTimeout, controlPlaneRequestTimeout,
                DEFAULT_MAX_RETRY_FOR_CANCELING_EXTERNAL_WORKFLOW, Duration.ofSeconds(DEFAULT_INITIAL_SLEEP_FOR_DECISION_THROTTLE_IN_SEC),
                Duration.ofSeconds(DEFAULT_MAX_SLEEP_FOR_DECISION_THROTTLE_IN_SEC), DEFAULT_BACKOFF_COEFFICIENT_FOR_DECISION_THROTTLE, DEFAULT_DECIDER_AFFINITY_CONFIG);
    }

    public int getDataPlaneRequestTimeoutInMillis() {
        return (int) dataPlaneRequestTimeout.toMillis();
    }

    public int getPollingRequestTimeoutInMillis() {
        return (int) pollingRequestTimeout.toMillis();
    }

    public int getControlPlaneRequestTimeoutInMillis() {
        return (int) controlPlaneRequestTimeout.toMillis();
    }

    public int getInitialSleepForDecisionThrottleInMillis() {
        return (int) initialSleepForDecisionThrottle.toMillis();
    }

    public int getMaxSleepForDecisionThrottleInMillis() {
        return (int) maxSleepForDecisionThrottle.toMillis();
    }

    public static class SimpleWorkflowClientConfigBuilder {

        public SimpleWorkflowClientConfigBuilder() {
            pollingRequestTimeout = Duration.ofSeconds(DEFAULT_POLLING_REQUEST_TIMEOUT_IN_SEC);
            dataPlaneRequestTimeout = Duration.ofSeconds(DEFAULT_DATA_PLANE_REQUEST_TIMEOUT_IN_SEC);
            controlPlaneRequestTimeout = Duration.ofSeconds(DEFAULT_CONTROL_PLANE_REQUEST_TIMEOUT_IN_SEC);
            maxRetryForCancelingExternalWorkflow = DEFAULT_MAX_RETRY_FOR_CANCELING_EXTERNAL_WORKFLOW;
            initialSleepForDecisionThrottle = Duration.ofSeconds(DEFAULT_INITIAL_SLEEP_FOR_DECISION_THROTTLE_IN_SEC);
            maxSleepForDecisionThrottle = Duration.ofSeconds(DEFAULT_MAX_SLEEP_FOR_DECISION_THROTTLE_IN_SEC);
            backoffCoefficientForDecisionThrottle = DEFAULT_BACKOFF_COEFFICIENT_FOR_DECISION_THROTTLE;
            deciderAffinityConfig = DEFAULT_DECIDER_AFFINITY_CONFIG;
        }
    }

}
