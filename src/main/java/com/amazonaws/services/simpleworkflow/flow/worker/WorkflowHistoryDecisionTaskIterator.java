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

import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution.fromSdkType;

import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.GetWorkflowExecutionHistoryResponse;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;

public class WorkflowHistoryDecisionTaskIterator implements Iterator<PollForDecisionTaskResponse> {

    private final SwfClient service;

    private final String domain;

    private final SimpleWorkflowClientConfig config;

    private final PollForDecisionTaskResponse decisionTask;

    private PollForDecisionTaskResponse next;

    private boolean initialized;

    public WorkflowHistoryDecisionTaskIterator(SwfClient service, String domain, PollForDecisionTaskResponse decisionTask, SimpleWorkflowClientConfig config) {
        this.service = service;
        this.domain = domain;
        this.decisionTask = decisionTask;
        this.config = config;
    }

    @Override
    public boolean hasNext() {
        if (!initialized) {
            initNext();
        }

        if (next == null) {
            return false;
        }
        List<HistoryEvent> events = next.events();
        if (events.size() == 0) {
            return false;
        }
        return true;
    }

    @Override
    public PollForDecisionTaskResponse next() {
        if (!hasNext()) {
            throw new IllegalStateException("hasNext() == false");
        }
        PollForDecisionTaskResponse result = next;
        if (next.nextPageToken() == null) {
            next = null;
        } else {
            next = getNextHistoryTask(next.nextPageToken());
        }
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void initNext() {
        initialized = true;
        next = getNextHistoryTask(null);
    }

    private PollForDecisionTaskResponse getNextHistoryTask(String nextPageToken) {
        GetWorkflowExecutionHistoryResponse history = WorkflowExecutionUtils.getHistoryPage(nextPageToken, service, domain,
            fromSdkType(decisionTask.workflowExecution()), config);
        List<HistoryEvent> events = history.events();
        if (events == null) {
            return null;
        }
        // Exclude history events that happened after this decision task
        List<HistoryEvent> eventsForDecisionTask = events.stream()
                .filter(event -> event.eventId().longValue() <= decisionTask.startedEventId().longValue())
                .collect(Collectors.toList());
        return decisionTask.toBuilder().events(eventsForDecisionTask)
                .nextPageToken(history.nextPageToken()).build();
    }

}
