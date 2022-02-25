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

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;

public class RequestTimeoutHelper {
    public static void overrideDataPlaneRequestTimeout(AmazonWebServiceRequest serviceRequest, SimpleWorkflowClientConfig config) {
        if (serviceRequest != null && config != null) {
            serviceRequest.setSdkRequestTimeout(config.getDataPlaneRequestTimeoutInMillis());
        }
    }

    public static void overrideControlPlaneRequestTimeout(AmazonWebServiceRequest serviceRequest, SimpleWorkflowClientConfig config) {
        if (serviceRequest != null && config != null) {
            serviceRequest.setSdkRequestTimeout(config.getControlPlaneRequestTimeoutInMillis());
        }
    }

    public static void overridePollRequestTimeout(AmazonWebServiceRequest serviceRequest, SimpleWorkflowClientConfig config) {
        if (serviceRequest != null && config != null) {
            serviceRequest.setSdkRequestTimeout(config.getPollingRequestTimeoutInMillis());
        }
    }
}
