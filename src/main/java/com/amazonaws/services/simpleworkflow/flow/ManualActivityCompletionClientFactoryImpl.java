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
package com.amazonaws.services.simpleworkflow.flow;

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;


public class ManualActivityCompletionClientFactoryImpl extends ManualActivityCompletionClientFactory {

    private AmazonSimpleWorkflow service;

    private DataConverter dataConverter = new JsonDataConverter();

    private SimpleWorkflowClientConfig config;

    public ManualActivityCompletionClientFactoryImpl(AmazonSimpleWorkflow service) {
        this(service, null);
    }

    public ManualActivityCompletionClientFactoryImpl(AmazonSimpleWorkflow service, SimpleWorkflowClientConfig config) {
        this.service = service;
        this.config = config;
    }

    public AmazonSimpleWorkflow getService() {
        return service;
    }

    public void setService(AmazonSimpleWorkflow service) {
        this.service = service;
    }

    public DataConverter getDataConverter() {
        return dataConverter;
    }

    public void setDataConverter(DataConverter dataConverter) {
        this.dataConverter = dataConverter;
    }

    public SimpleWorkflowClientConfig getClientConfig() {
        return config;
    }

    public void setClientConfig(SimpleWorkflowClientConfig config) {
        this.config = config;
    }

    @Override
    public ManualActivityCompletionClient getClient(String taskToken) {
        if (service == null) {
            throw new IllegalStateException("required property service is null");
        }
        if (dataConverter == null) {
            throw new IllegalStateException("required property dataConverter is null");
        }
        return new ManualActivityCompletionClientImpl(service, taskToken, dataConverter, config);
    }

}
