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
package com.amazonaws.services.simpleworkflow.flow.worker;

import com.amazonaws.services.simpleworkflow.flow.ActivityExecutionContext;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricHelper;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.Metrics;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.RecordActivityTaskHeartbeatRequest;
import software.amazon.awssdk.services.swf.model.RecordActivityTaskHeartbeatResponse;

/**
 * Base implementation of an {@link ActivityExecutionContext}.
 * 
 * @see ActivityExecutionContext
 * 
 */
class ActivityExecutionContextImpl extends ActivityExecutionContext {

    @Getter
    private final SwfClient service;

    @Getter
    private final String domain;

    private final SimpleWorkflowClientConfig config;

    private final MetricsRegistry metricsRegistry;

    /**
     * Create an ActivityExecutionContextImpl with the given attributes.
     * 
     * @param service
     *            The {@link SwfClient} this
     *            ActivityExecutionContextImpl will send service calls to.
     * @param task
     *            The {@link ActivityTask} this ActivityExecutionContextImpl
     *            will be used for.
     * 
     * @see ActivityExecutionContext
     */
    public ActivityExecutionContextImpl(SwfClient service, String domain, ActivityTask task) {
        this(service, domain, task, null, ThreadLocalMetrics.getMetrics().getMetricsRegistry());
    }

    /**
     * Create an ActivityExecutionContextImpl with the given attributes.
     *
     * @param service
     *            The {@link SwfClient} this
     *            ActivityExecutionContextImpl will send service calls to.
     * @param task
     *            The {@link ActivityTask} this ActivityExecutionContextImpl
     *            will be used for.
     *
     * @see ActivityExecutionContext
     */
    public ActivityExecutionContextImpl(SwfClient service, String domain, ActivityTask task, SimpleWorkflowClientConfig config, MetricsRegistry metricsRegistry) {
        super(task);
        this.domain = domain;
        this.service = service;
        this.config = config;
        this.metricsRegistry = metricsRegistry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordActivityHeartbeat(String details) throws CancellationException {
        //TODO: call service with the specified minimal interval (through @ActivityExecutionOptions)
        // allowing more frequent calls of this method.
        RecordActivityTaskHeartbeatRequest recordActivityTaskHeartbeatRequest = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(
            RecordActivityTaskHeartbeatRequest.builder().taskToken(getTask().getTaskToken()).details(details)
                .build(), config);

        RecordActivityTaskHeartbeatResponse status;
        /*
        We associate every SWF API request metrics log entry with information from SDK like Retry Info, Invocation ID, etc.
        If we have 1 metrics log entry contain information from 2 different SWF API requests, it'd be impossible to know
        which SDK metrics relate to which of the 2 SWF API requests so, we create a new metrics log entry for every
        SWF API request.
        Long-running Activities also benefit from having separate metrics log entry for every heartbeat as otherwise,
        no metrics will be emitted until the long-running activity completes.

        Customers will be able to easily find all metrics log entries for a given Task or Workflow Execution since each
        metrics log entry contains enough information like Task Token, Workflow Execution, etc.
         */
        try (Metrics metrics = metricsRegistry.newMetrics(MetricName.Operation.EXECUTE_ACTIVITY_TASK.getName())) {
            MetricHelper.recordMetrics(getTask(), metrics);
            status = metrics.recordSupplier(() -> service.recordActivityTaskHeartbeat(recordActivityTaskHeartbeatRequest),
                MetricName.Operation.RECORD_ACTIVITY_TASK_HEARTBEAT.getName(), TimeUnit.MILLISECONDS);
            final boolean isCancelRequested = status.cancelRequested();
            metrics.recordCount(MetricName.ACTIVITY_CANCEL_REQUESTED.getName(), isCancelRequested);
            if (isCancelRequested) {
                throw new CancellationException();
            }
        }
    }
}
