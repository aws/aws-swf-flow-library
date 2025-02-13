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
package com.amazonaws.services.simpleworkflow.flow;

import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import software.amazon.awssdk.services.swf.SwfClient;


public class ManualActivityCompletionClientFactoryImpl extends ManualActivityCompletionClientFactory {

    private SwfClient service;

    private DataConverter dataConverter = new JsonDataConverter();

    private SimpleWorkflowClientConfig config;
    
    public ManualActivityCompletionClientFactoryImpl(SwfClient service) {
        this(service, null);
    }

    public ManualActivityCompletionClientFactoryImpl(SwfClient service, SimpleWorkflowClientConfig config) {
        this.service = service;
        this.config = config;
    }

    public SwfClient getService() {
        return service;
    }
    
    public void setService(SwfClient service) {
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
