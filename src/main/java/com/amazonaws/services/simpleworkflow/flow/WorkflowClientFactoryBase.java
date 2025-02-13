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

import com.amazonaws.services.simpleworkflow.flow.generic.GenericWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;

public abstract class WorkflowClientFactoryBase<T> implements WorkflowClientFactory<T> {

    private GenericWorkflowClient genericClient;

    private DataConverter dataConverter;

    private StartWorkflowOptions startWorkflowOptions = new StartWorkflowOptions();

    private final DecisionContextProvider decisionContextProvider = new DecisionContextProviderImpl();

    public WorkflowClientFactoryBase() {
        this(null, null, null);
    }

    public WorkflowClientFactoryBase(StartWorkflowOptions startWorkflowOptions) {
        this(startWorkflowOptions, null, null);
    }

    public WorkflowClientFactoryBase(StartWorkflowOptions startWorkflowOptions, DataConverter dataConverter) {
        this(startWorkflowOptions, dataConverter, null);
    }

    public WorkflowClientFactoryBase(StartWorkflowOptions startWorkflowOptions, DataConverter dataConverter,
            GenericWorkflowClient genericClient) {
        this.startWorkflowOptions = startWorkflowOptions;
        if (dataConverter == null) {
            this.dataConverter = new JsonDataConverter();
        }
        else {
            this.dataConverter = dataConverter;
        }
        this.genericClient = genericClient;
    }

    @Override
    public GenericWorkflowClient getGenericClient() {
        return genericClient;
    }

    @Override
    public void setGenericClient(GenericWorkflowClient genericClient) {
        this.genericClient = genericClient;
    }

    @Override
    public DataConverter getDataConverter() {
        return dataConverter;
    }

    @Override
    public void setDataConverter(DataConverter dataConverter) {
        this.dataConverter = dataConverter;
    }

    @Override
    public StartWorkflowOptions getStartWorkflowOptions() {
        return startWorkflowOptions;
    }

    @Override
    public void setStartWorkflowOptions(StartWorkflowOptions startWorkflowOptions) {
        this.startWorkflowOptions = startWorkflowOptions;
    }

    @Override
    public T getClient() {
    	GenericWorkflowClient client = getGenericClientToUse();
        String workflowId = client.generateUniqueId();
        WorkflowExecution execution = WorkflowExecution.builder().workflowId(workflowId).build();
        return getClient(execution, startWorkflowOptions, dataConverter);
    }

    @Override
    public T getClient(String workflowId) {
        if (workflowId == null || workflowId.isEmpty()) {
            throw new IllegalArgumentException("workflowId");
        }
        WorkflowExecution execution = WorkflowExecution.builder().workflowId(workflowId).build();
        return getClient(execution, startWorkflowOptions, dataConverter);
    }

    @Override
    public T getClient(WorkflowExecution execution) {
        return getClient(execution, startWorkflowOptions, dataConverter);
    }

    @Override
    public T getClient(WorkflowExecution execution, StartWorkflowOptions options) {
        return getClient(execution, options, dataConverter);
    }

    @Override
    public T getClient(WorkflowExecution execution, StartWorkflowOptions options, DataConverter dataConverter) {
        GenericWorkflowClient client = getGenericClientToUse();
		return createClientInstance(execution, options, dataConverter, client);
    }

	private GenericWorkflowClient getGenericClientToUse() {
		GenericWorkflowClient result;
        if (genericClient == null) {
            result = decisionContextProvider.getDecisionContext().getWorkflowClient();
        } else {
        	result = genericClient;
        }
        return result;
	}

    protected abstract T createClientInstance(WorkflowExecution execution, StartWorkflowOptions options,
            DataConverter dataConverter, GenericWorkflowClient genericClient);

}
