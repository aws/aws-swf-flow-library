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

public interface WorkflowClientFactory<T> {

    GenericWorkflowClient getGenericClient();

    void setGenericClient(GenericWorkflowClient genericClient);

    DataConverter getDataConverter();

    void setDataConverter(DataConverter dataConverter);

    StartWorkflowOptions getStartWorkflowOptions();

    void setStartWorkflowOptions(StartWorkflowOptions startWorkflowOptions);

    T getClient();

    T getClient(String workflowId);

    T getClient(WorkflowExecution execution);

    T getClient(WorkflowExecution execution, StartWorkflowOptions options);

    T getClient(WorkflowExecution execution, StartWorkflowOptions options, DataConverter dataConverter);

}
