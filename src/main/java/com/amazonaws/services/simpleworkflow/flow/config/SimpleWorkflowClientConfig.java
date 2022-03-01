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

import java.time.Duration;

public class SimpleWorkflowClientConfig {
    private final Duration dataPlaneRequestTimeout;
    private final Duration pollingRequestTimeout;
    private final Duration controlPlaneRequestTimeout;
    private static final int DEFAULT_POLLING_REQUEST_TIMEOUT_IN_SEC = 70;
    private static final int DEFAULT_DATA_PLANE_REQUEST_TIMEOUT_IN_SEC = 6;
    private static final int DEFAULT_CONTROL_PLANE_REQUEST_TIMEOUT_IN_SEC = 10;

    /**
     * Class method to provide SimpleWorkflowClientConfig with default timeouts
     *
     * @return SimpleWorkflowClientConfig instance
     */
    public static SimpleWorkflowClientConfig ofDefaults() {
        return new SimpleWorkflowClientConfig(Duration.ofSeconds(DEFAULT_POLLING_REQUEST_TIMEOUT_IN_SEC),
                Duration.ofSeconds(DEFAULT_DATA_PLANE_REQUEST_TIMEOUT_IN_SEC),
                Duration.ofSeconds(DEFAULT_CONTROL_PLANE_REQUEST_TIMEOUT_IN_SEC));
    }

    /**
     * Create SimpleWorkflowClientConfig with polling/dataPlane/controlPlane timeouts
     *
     * @param pollingRequestTimeout timeout for polling APIs like PollForActivityTask, PollForDecisionTask
     * @param dataPlaneRequestTimeout timeout for dataPlane APIs like StartWorkflowExecution, RespondActivityTaskCompleted
     * @param controlPlaneRequestTimeout timeout for controlPlane APIs like ListDomains, ListActivityTypes
     */
    public SimpleWorkflowClientConfig(Duration pollingRequestTimeout, Duration dataPlaneRequestTimeout, Duration controlPlaneRequestTimeout) {
        this.pollingRequestTimeout = pollingRequestTimeout;
        this.dataPlaneRequestTimeout = dataPlaneRequestTimeout;
        this.controlPlaneRequestTimeout = controlPlaneRequestTimeout;
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
}
