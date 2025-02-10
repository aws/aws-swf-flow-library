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

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.core.AsyncTaskInfo;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.replaydeserializer.TimeStampMixin;
import com.amazonaws.services.simpleworkflow.flow.worker.AsyncDecisionTaskHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.GetWorkflowExecutionHistoryResponse;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.RespondDecisionTaskCompletedRequest;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionStartedEventAttributes;

public class WorkflowReplayer<T> {

    private final class WorkflowReplayerPOJOFactoryFactory extends POJOWorkflowDefinitionFactoryFactory {

        private final T workflowImplementation;

        private WorkflowReplayerPOJOFactoryFactory(T workflowImplementation) throws InstantiationException, IllegalAccessException {
            this.workflowImplementation = workflowImplementation;
            super.addWorkflowImplementationType(workflowImplementation.getClass());
        }

        @Override
        protected POJOWorkflowImplementationFactory getImplementationFactory(Class<?> workflowImplementationType,
                Class<?> workflowInteface, WorkflowType workflowType) {
            return new POJOWorkflowImplementationFactory() {

                @Override
                public Object newInstance(DecisionContext decisionContext) throws Exception {
                    return workflowImplementation;
                }

                @Override
                public Object newInstance(DecisionContext decisionContext, Object[] constructorArgs) throws Exception {
                    return workflowImplementation;
                }

                @Override
                public void deleteInstance(Object instance) {
                }
            };
        }
    }

    private abstract class DecisionTaskIterator implements Iterator<PollForDecisionTaskResponse> {

        private PollForDecisionTaskResponse next;
        
        private boolean initialized;

        protected void initNext() {
            initialized = true;
            next = getNextHistoryTask(null);
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
            if (replayUpToEventId == 0) {
                return true;
            }
            HistoryEvent firstEvent = next.events().get(0);
            return firstEvent.eventId() <= replayUpToEventId;
        }

        @Override
        public PollForDecisionTaskResponse next() {
            if (!hasNext()) {
                throw new IllegalStateException("hasNext() == false");
            }
            PollForDecisionTaskResponse result = next;
            if (next.nextPageToken() == null) {
                next = null;
            }
            else {
                next = getNextHistoryTask(next.nextPageToken());
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        protected List<HistoryEvent> truncateHistory(List<HistoryEvent> events) {
            if (events.size() == 0) {
                return null;
            }
            if (replayUpToEventId == 0) {
                return events;
            }
            HistoryEvent lastEvent = events.get(events.size() - 1);
            if (lastEvent.eventId() <= replayUpToEventId) {
                return events;
            }
            List<HistoryEvent> truncated = new ArrayList<HistoryEvent>();
            for (HistoryEvent event : events) {
                if (event.eventId() > replayUpToEventId) {
                    break;
                }
                truncated.add(event);
            }
            if (truncated.size() == 0) {
                return null;
            }
            return truncated;
        }

        protected abstract PollForDecisionTaskResponse getNextHistoryTask(String nextPageToken);

    }

    private class ServiceDecisionTaskIterator extends DecisionTaskIterator {

        private final SwfClient service;

        private final String domain;

        private final WorkflowExecution workflowExecution;

        private final SimpleWorkflowClientConfig config;

        public ServiceDecisionTaskIterator(SwfClient service, String domain, WorkflowExecution workflowExecution) {
            this(service, domain, workflowExecution, null);
        }

        public ServiceDecisionTaskIterator(SwfClient service, String domain, WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) {
            this.service = service;
            this.domain = domain;
            this.workflowExecution = workflowExecution;
            this.config = config;
        }

        protected PollForDecisionTaskResponse getNextHistoryTask(String nextPageToken) {
            WorkflowExecutionInfo executionInfo = WorkflowExecutionUtils.describeWorkflowInstance(service, domain,
                    workflowExecution, config);
            GetWorkflowExecutionHistoryResponse history = WorkflowExecutionUtils.getHistoryPage(nextPageToken, service, domain, workflowExecution, config);

            List<HistoryEvent> events = history.events();
            events = truncateHistory(events);
            if (events == null) {
                return null;
            }
            return PollForDecisionTaskResponse.builder()
                .events(events)
                .workflowExecution(workflowExecution.toSdkType())
                .workflowType(executionInfo.workflowType())
                .nextPageToken(history.nextPageToken())
                .build();
        }

    }

    private class HistoryIterableDecisionTaskIterator extends DecisionTaskIterator {

        private final WorkflowExecution workflowExecution;

        private final Iterable<HistoryEvent> history;

        public HistoryIterableDecisionTaskIterator(WorkflowExecution workflowExecution, Iterable<HistoryEvent> history) {
            this.workflowExecution = workflowExecution;
            this.history = history;
        }

        @Override
        protected PollForDecisionTaskResponse getNextHistoryTask(String nextPageToken) {
            Iterator<HistoryEvent> iterator = history.iterator();
            if (!iterator.hasNext()) {
                throw new IllegalStateException("empty history");
            }
            HistoryEvent startEvent = iterator.next();
            WorkflowExecutionStartedEventAttributes startedAttributes = startEvent.workflowExecutionStartedEventAttributes();
            if (startedAttributes == null) {
                throw new IllegalStateException("first event is not WorkflowExecutionStarted: " + startEvent);
            }
            List<HistoryEvent> events = new ArrayList<HistoryEvent>();
            events.add(startEvent);
            EventType eventType = null;
            int lastStartedIndex = 0;
            int index = 1;
            long previousStartedEventId = 0;
            long startedEventId = 0;
            while (iterator.hasNext()) {
                HistoryEvent event = iterator.next();
                eventType = EventType.fromValue(event.eventTypeAsString());
                events.add(event);
                if (eventType == EventType.DECISION_TASK_STARTED) {
                    previousStartedEventId = startedEventId;
                    startedEventId = event.eventId();
                    lastStartedIndex = index;
                }
                index++;
            }
            if (events.size() > lastStartedIndex + 1) {
                events = events.subList(0, lastStartedIndex + 1);
            }
            return PollForDecisionTaskResponse.builder()
                .events(events)
                .previousStartedEventId(previousStartedEventId)
                .startedEventId(startedEventId)
                .workflowExecution(workflowExecution.toSdkType())
                .workflowType(startedAttributes.workflowType())
                .build();
        }
    }

    private final Iterator<PollForDecisionTaskResponse> taskIterator;

    private final AsyncDecisionTaskHandler taskHandler;

    private int replayUpToEventId;
    
    private final AtomicBoolean replayed = new AtomicBoolean();

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
            Class<T> workflowImplementationType) throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowImplementationType, null);
    }

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
            Class<T> workflowImplementationType, SimpleWorkflowClientConfig config) throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowImplementationType, config, null);
    }

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
        Class<T> workflowImplementationType, SimpleWorkflowClientConfig config, ChildWorkflowIdHandler childWorkflowIdHandler) throws InstantiationException, IllegalAccessException {
        POJOWorkflowDefinitionFactoryFactory ff = new POJOWorkflowDefinitionFactoryFactory();
        ff.addWorkflowImplementationType(workflowImplementationType);
        taskIterator = new ServiceDecisionTaskIterator(service, domain, workflowExecution, config);
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
            final T workflowImplementation) throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowImplementation, null);
    }

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
            final T workflowImplementation, SimpleWorkflowClientConfig config) throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowImplementation, null, null);
    }

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
        final T workflowImplementation, SimpleWorkflowClientConfig config, ChildWorkflowIdHandler childWorkflowIdHandler) throws InstantiationException, IllegalAccessException {
        WorkflowDefinitionFactoryFactory ff = new WorkflowReplayerPOJOFactoryFactory(workflowImplementation);
        taskIterator = new ServiceDecisionTaskIterator(service, domain, workflowExecution, config);
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory)
            throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowDefinitionFactoryFactory, null);
    }

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory, SimpleWorkflowClientConfig config)
            throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowDefinitionFactoryFactory, config, null);
    }

    public WorkflowReplayer(SwfClient service, String domain, WorkflowExecution workflowExecution,
        WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory, SimpleWorkflowClientConfig config,
        ChildWorkflowIdHandler childWorkflowIdHandler)
        throws InstantiationException, IllegalAccessException {
        taskIterator = new ServiceDecisionTaskIterator(service, domain, workflowExecution, config);
        taskHandler = new AsyncDecisionTaskHandler(workflowDefinitionFactoryFactory, childWorkflowIdHandler);
    }

    public WorkflowReplayer(Iterable<HistoryEvent> history, WorkflowExecution workflowExecution,
            Class<T> workflowImplementationType) throws InstantiationException, IllegalAccessException {
        this(history, workflowExecution, workflowImplementationType, false);
    }

    public WorkflowReplayer(Iterable<HistoryEvent> history, WorkflowExecution workflowExecution,
            Class<T> workflowImplementationType, boolean skipFailedCheck) throws InstantiationException, IllegalAccessException {
        this(history, workflowExecution, workflowImplementationType, skipFailedCheck, null);
    }

    public WorkflowReplayer(Iterable<HistoryEvent> history, WorkflowExecution workflowExecution,
        Class<T> workflowImplementationType, boolean skipFailedCheck, ChildWorkflowIdHandler childWorkflowIdHandler) throws InstantiationException, IllegalAccessException {
        POJOWorkflowDefinitionFactoryFactory ff = new POJOWorkflowDefinitionFactoryFactory();
        ff.addWorkflowImplementationType(workflowImplementationType);
        taskIterator = new HistoryIterableDecisionTaskIterator(workflowExecution, history);
        taskHandler = new AsyncDecisionTaskHandler(ff, skipFailedCheck, childWorkflowIdHandler);
    }

    public WorkflowReplayer(Iterable<HistoryEvent> history, WorkflowExecution workflowExecution, final T workflowImplementation)
            throws InstantiationException, IllegalAccessException {
        this(history, workflowExecution, workflowImplementation, null);
    }

    public WorkflowReplayer(Iterable<HistoryEvent> history, WorkflowExecution workflowExecution, final T workflowImplementation, ChildWorkflowIdHandler childWorkflowIdHandler)
        throws InstantiationException, IllegalAccessException {
        WorkflowDefinitionFactoryFactory ff = new WorkflowReplayerPOJOFactoryFactory(workflowImplementation);
        taskIterator = new HistoryIterableDecisionTaskIterator(workflowExecution, history);
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(Iterable<HistoryEvent> history, WorkflowExecution workflowExecution,
            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory)
            throws InstantiationException, IllegalAccessException {
        this(history, workflowExecution, workflowDefinitionFactoryFactory, null);
    }

    public WorkflowReplayer(Iterable<HistoryEvent> history, WorkflowExecution workflowExecution,
        WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory, ChildWorkflowIdHandler childWorkflowIdHandler)
        throws InstantiationException, IllegalAccessException {
        taskIterator = new HistoryIterableDecisionTaskIterator(workflowExecution, history);
        taskHandler = new AsyncDecisionTaskHandler(workflowDefinitionFactoryFactory, childWorkflowIdHandler);
    }

    public WorkflowReplayer(String historyEvents, WorkflowExecution workflowExecution,
                            Class<T> workflowImplementationType) throws InstantiationException, IllegalAccessException, IOException {
        this(historyEvents, workflowExecution, workflowImplementationType, false);
    }

    public WorkflowReplayer(String historyEvents, WorkflowExecution workflowExecution,
                            Class<T> workflowImplementationType, boolean skipFailedCheck) throws InstantiationException, IllegalAccessException, IOException {
        this(historyEvents, workflowExecution, workflowImplementationType, skipFailedCheck, null);
    }

    public WorkflowReplayer(String historyEvents, WorkflowExecution workflowExecution,
        Class<T> workflowImplementationType, boolean skipFailedCheck, ChildWorkflowIdHandler childWorkflowIdHandler) throws InstantiationException, IllegalAccessException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.addMixIn(HistoryEvent.class, TimeStampMixin.class);
        ObjectReader reader = mapper.readerFor(new TypeReference<List<HistoryEvent>>() {}).withRootName("events");
        List<HistoryEvent> history = reader.readValue(historyEvents);

        POJOWorkflowDefinitionFactoryFactory ff = new POJOWorkflowDefinitionFactoryFactory();
        ff.addWorkflowImplementationType(workflowImplementationType);
        taskIterator = new HistoryIterableDecisionTaskIterator(workflowExecution, history);
        taskHandler = new AsyncDecisionTaskHandler(ff, skipFailedCheck, childWorkflowIdHandler);
    }

    public WorkflowReplayer(String historyEvents, WorkflowExecution workflowExecution, final T workflowImplementation)
            throws InstantiationException, IllegalAccessException, IOException {
        this(historyEvents, workflowExecution, workflowImplementation, null);
    }

    public WorkflowReplayer(String historyEvents, WorkflowExecution workflowExecution, final T workflowImplementation, ChildWorkflowIdHandler childWorkflowIdHandler)
        throws InstantiationException, IllegalAccessException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.addMixIn(HistoryEvent.class, TimeStampMixin.class);
        ObjectReader reader = mapper.readerFor(new TypeReference<List<HistoryEvent>>() {}).withRootName("events");
        List<HistoryEvent> history = reader.readValue(historyEvents);

        WorkflowDefinitionFactoryFactory ff = new WorkflowReplayerPOJOFactoryFactory(workflowImplementation);
        taskIterator = new HistoryIterableDecisionTaskIterator(workflowExecution, history);
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(String historyEvents, WorkflowExecution workflowExecution,
                            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory) throws IOException {
        this(historyEvents, workflowExecution, workflowDefinitionFactoryFactory, null);
    }

    public WorkflowReplayer(String historyEvents, WorkflowExecution workflowExecution,
        WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory, ChildWorkflowIdHandler childWorkflowIdHandler) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.addMixIn(HistoryEvent.class, TimeStampMixin.class);
        ObjectReader reader = mapper.readerFor(new TypeReference<List<HistoryEvent>>() {}).withRootName("events");
        List<HistoryEvent> history = reader.readValue(historyEvents);

        taskIterator = new HistoryIterableDecisionTaskIterator(workflowExecution, history);
        taskHandler = new AsyncDecisionTaskHandler(workflowDefinitionFactoryFactory, childWorkflowIdHandler);
    }

    public WorkflowReplayer(Iterator<PollForDecisionTaskResponse> decisionTasks, Class<T> workflowImplementationType)
            throws InstantiationException, IllegalAccessException {
        this(decisionTasks, workflowImplementationType, null);
    }

    public WorkflowReplayer(Iterator<PollForDecisionTaskResponse> decisionTasks, Class<T> workflowImplementationType, ChildWorkflowIdHandler childWorkflowIdHandler)
        throws InstantiationException, IllegalAccessException {
        POJOWorkflowDefinitionFactoryFactory ff = new POJOWorkflowDefinitionFactoryFactory();
        ff.addWorkflowImplementationType(workflowImplementationType);
        taskIterator = decisionTasks;
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(Iterator<PollForDecisionTaskResponse> decisionTasks, final T workflowImplementation)
            throws InstantiationException, IllegalAccessException {
        this(decisionTasks, workflowImplementation, null);
    }

    public WorkflowReplayer(Iterator<PollForDecisionTaskResponse> decisionTasks, final T workflowImplementation, ChildWorkflowIdHandler childWorkflowIdHandler)
        throws InstantiationException, IllegalAccessException {
        WorkflowDefinitionFactoryFactory ff = new WorkflowReplayerPOJOFactoryFactory(workflowImplementation);
        taskIterator = decisionTasks;
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(Iterator<PollForDecisionTaskResponse> decisionTasks,
            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory)
            throws InstantiationException, IllegalAccessException {
        this(decisionTasks, workflowDefinitionFactoryFactory, null);
    }

    public WorkflowReplayer(Iterator<PollForDecisionTaskResponse> decisionTasks,
        WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory, ChildWorkflowIdHandler childWorkflowIdHandler)
        throws InstantiationException, IllegalAccessException {
        taskIterator = decisionTasks;
        taskHandler = new AsyncDecisionTaskHandler(workflowDefinitionFactoryFactory, childWorkflowIdHandler);
    }

    public int getReplayUpToEventId() {
        return replayUpToEventId;
    }

    /**
     * The replay stops at the event with the given eventId. Default is 0.
     * 
     * @param replayUpToEventId
     *            0 means the whole history.
     */
    public void setReplayUpToEventId(int replayUpToEventId) {
        this.replayUpToEventId = replayUpToEventId;
    }

    public RespondDecisionTaskCompletedRequest replay() throws Exception {
        checkReplayed();
        return taskHandler.handleDecisionTask(taskIterator).getRespondDecisionTaskCompletedRequest();
    }

    @SuppressWarnings("unchecked")
    public T loadWorkflow() throws Exception {
        checkReplayed();
        WorkflowDefinition definition = taskHandler.loadWorkflowThroughReplay(taskIterator);
        POJOWorkflowDefinition pojoDefinition = (POJOWorkflowDefinition) definition;
        return (T) pojoDefinition.getImplementationInstance();
    }

    public List<AsyncTaskInfo> getAsynchronousThreadDump() throws Exception {
        checkReplayed();
        return taskHandler.getAsynchronousThreadDump(taskIterator);
    }

    public String getAsynchronousThreadDumpAsString() throws Exception {
        checkReplayed();
        return taskHandler.getAsynchronousThreadDumpAsString(taskIterator);
    }
    
    private void checkReplayed() {
        if (!replayed.compareAndSet(false, true)) {
            throw new IllegalStateException("WorkflowReplayer instance can be used only once.");
        }
    }
}
