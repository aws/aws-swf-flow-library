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
package com.amazonaws.services.simpleworkflow.flow.common;

import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;

import java.net.SocketException;
import java.time.Duration;
import java.util.function.Predicate;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.swf.model.SwfRequest;

public class RequestTimeoutHelper {

    private static final SimpleWorkflowClientConfig DEFAULT_CLIENT_CONFIG = SimpleWorkflowClientConfig.ofDefaults();

    public static final Predicate<SdkClientException> BROKEN_PIPE_ERROR_PREDICATE = ex -> ex.getCause() instanceof SocketException &&
        ex.getCause().getMessage().contains("Broken pipe");

    public static <T extends SwfRequest> T overrideDataPlaneRequestTimeout(T serviceRequest, SimpleWorkflowClientConfig config) {
        return overrideApiCallTimeout(serviceRequest, config != null
            ? config.getDataPlaneRequestTimeoutInMillis()
            : DEFAULT_CLIENT_CONFIG.getDataPlaneRequestTimeoutInMillis());
    }

    public static <T extends SwfRequest> T overrideControlPlaneRequestTimeout(T serviceRequest, SimpleWorkflowClientConfig config) {
        return overrideApiCallTimeout(serviceRequest, config != null
            ? config.getControlPlaneRequestTimeoutInMillis()
            : DEFAULT_CLIENT_CONFIG.getControlPlaneRequestTimeoutInMillis());
    }

    public static <T extends SwfRequest> T overridePollRequestTimeout(T serviceRequest, SimpleWorkflowClientConfig config) {
        return overrideApiCallTimeout(serviceRequest, config != null
            ? config.getPollingRequestTimeoutInMillis()
            : DEFAULT_CLIENT_CONFIG.getPollingRequestTimeoutInMillis());
    }

    public static <T extends SwfRequest> T overrideApiCallTimeout(T serviceRequest, int requestCallTimeout) {
        if (serviceRequest != null) {
            AwsRequestOverrideConfiguration configuration = AwsRequestOverrideConfiguration.builder()
                .apiCallAttemptTimeout(Duration.ofMillis(requestCallTimeout)).build();
            return (T) serviceRequest.toBuilder().overrideConfiguration(configuration).build();
        }
        return serviceRequest;
    }
}
