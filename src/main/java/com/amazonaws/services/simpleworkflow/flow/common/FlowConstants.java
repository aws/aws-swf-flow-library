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
package com.amazonaws.services.simpleworkflow.flow.common;

public final class FlowConstants {

    public static final int NONE = -1;

    public static final int USE_REGISTERED_DEFAULTS = -2;

    /**
     * Do not specify task list on registration. Which means that task list is
     * required when scheduling activity.
     */
    public static final String NO_DEFAULT_TASK_LIST = "NO_DEFAULT_TASK_LIST";

    /**
     * Use task list of the {@link com.amazonaws.services.simpleworkflow.flow.ActivityWorker} or {@link com.amazonaws.services.simpleworkflow.flow.WorkflowWorker}
     * that is used to register activity or workflow as the default task list for
     * the activity or workflow type.
     */
    public static final String USE_WORKER_TASK_LIST = "USE_WORKER_TASK_LIST";

    /**
     * Use task priority 0
     */
    public static final int DEFAULT_TASK_PRIORITY = 0;

    public static final long DEFAULT_LAMBDA_FUNCTION_TIMEOUT = 300;
    
    /**
     *  ExponentialRetryWithJitterPolicy defaults
     */
    public static final int EXPONENTIAL_INITIAL_RETRY_INTERVAL_SECONDS = 5;

    public static final int EXPONENTIAL_RETRY_MAXIMUM_ATTEMPTS = -1;

    public static final long EXPONENTIAL_RETRY_MAXIMUM_RETRY_INTERVAL_SECONDS = -1;

    public static final long EXPONENTIAL_RETRY_EXPIRATION_SECONDS = -1;

    public static final double EXPONENTIAL_RETRY_BACKOFF_COEFFICIENT = 2.0;

    public static final double EXPONENTIAL_RETRY_MAXIMUM_JITTER_COEFFICIENT = 0.5;

}
