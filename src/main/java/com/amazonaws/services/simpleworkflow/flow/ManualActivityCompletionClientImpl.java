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

import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.RecordActivityTaskHeartbeatRequest;
import software.amazon.awssdk.services.swf.model.RecordActivityTaskHeartbeatResponse;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCanceledRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCompletedRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskFailedRequest;

/**
 * TODO: Add exponential retry to manual activity completion the same way it is
 * done for other activities
 */
class ManualActivityCompletionClientImpl extends ManualActivityCompletionClient {

    private final SwfClient service;

    private final String taskToken;

    private final DataConverter dataConverter;

    private SimpleWorkflowClientConfig config;

    public ManualActivityCompletionClientImpl(SwfClient service, String taskToken, DataConverter dataConverter) {
        this(service, taskToken, dataConverter, null);
    }

    public ManualActivityCompletionClientImpl(SwfClient service, String taskToken, DataConverter dataConverter, SimpleWorkflowClientConfig config) {
        this.service = service;
        this.taskToken = taskToken;
        this.dataConverter = dataConverter;
        this.config = config;
    }

    @Override
    public void complete(Object result) {
        final String convertedResult = ThreadLocalMetrics.getMetrics().recordSupplier(
            () -> dataConverter.toData(result),
            dataConverter.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_SERIALIZE.getName(),
            TimeUnit.MILLISECONDS
        );

        RespondActivityTaskCompletedRequest request = RespondActivityTaskCompletedRequest.builder().taskToken(taskToken).result(convertedResult).build();
        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.respondActivityTaskCompleted(request);
    }

    @Override
    public void fail(Throwable failure) {
        final String convertedFailure = ThreadLocalMetrics.getMetrics().recordSupplier(
            () -> dataConverter.toData(failure),
            dataConverter.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_SERIALIZE.getName(),
            TimeUnit.MILLISECONDS
        );
        RespondActivityTaskFailedRequest request
            = RespondActivityTaskFailedRequest.builder().reason(WorkflowExecutionUtils.truncateReason(failure.getMessage()))
            .details(convertedFailure).taskToken(taskToken).build();

        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.respondActivityTaskFailed(request);
    }

    @Override
    public void recordHeartbeat(String details) throws CancellationException {
        RecordActivityTaskHeartbeatRequest request
            = RecordActivityTaskHeartbeatRequest.builder().details(details).taskToken(taskToken).build();
        RecordActivityTaskHeartbeatResponse status;

        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        status = service.recordActivityTaskHeartbeat(request);
        if (status.cancelRequested()) {
            throw new CancellationException();
        }
    }

    @Override
    public void reportCancellation(String details) {
        RespondActivityTaskCanceledRequest request
            = RespondActivityTaskCanceledRequest.builder().details(details).taskToken(taskToken).build();

        request = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(request, config);
        service.respondActivityTaskCanceled(request);
    }

}
