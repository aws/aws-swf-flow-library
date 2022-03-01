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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.core.AsyncTaskInfo;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.replaydeserializer.TimeStampMixin;
import com.amazonaws.services.simpleworkflow.flow.worker.AsyncDecisionTaskHandler;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.EventType;
import com.amazonaws.services.simpleworkflow.model.History;
import com.amazonaws.services.simpleworkflow.model.HistoryEvent;
import com.amazonaws.services.simpleworkflow.model.RespondDecisionTaskCompletedRequest;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfo;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionStartedEventAttributes;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

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

    private abstract class DecisionTaskIterator implements Iterator<DecisionTask> {

        private DecisionTask next;

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
            List<HistoryEvent> events = next.getEvents();
            if (events.size() == 0) {
                return false;
            }
            if (replayUpToEventId == 0) {
                return true;
            }
            HistoryEvent firstEvent = next.getEvents().get(0);
            return firstEvent.getEventId() <= replayUpToEventId;
        }

        @Override
        public DecisionTask next() {
            if (!hasNext()) {
                throw new IllegalStateException("hasNext() == false");
            }
            DecisionTask result = next;
            if (next.getNextPageToken() == null) {
                next = null;
            }
            else {
                next = getNextHistoryTask(next.getNextPageToken());
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
            if (lastEvent.getEventId() <= replayUpToEventId) {
                return events;
            }
            List<HistoryEvent> truncated = new ArrayList<HistoryEvent>();
            for (HistoryEvent event : events) {
                if (event.getEventId() > replayUpToEventId) {
                    break;
                }
                truncated.add(event);
            }
            if (truncated.size() == 0) {
                return null;
            }
            return truncated;
        }

        protected abstract DecisionTask getNextHistoryTask(String nextPageToken);

    }

    private class ServiceDecisionTaskIterator extends DecisionTaskIterator {

        private final AmazonSimpleWorkflow service;

        private final String domain;

        private final WorkflowExecution workflowExecution;

        private final SimpleWorkflowClientConfig config;

        public ServiceDecisionTaskIterator(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution) {
            this(service, domain, workflowExecution, null);
        }

        public ServiceDecisionTaskIterator(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution, SimpleWorkflowClientConfig config) {
            this.service = service;
            this.domain = domain;
            this.workflowExecution = workflowExecution;
            this.config = config;
        }

        protected DecisionTask getNextHistoryTask(String nextPageToken) {
            WorkflowExecutionInfo executionInfo = WorkflowExecutionUtils.describeWorkflowInstance(service, domain,
                    workflowExecution, config);
            History history = WorkflowExecutionUtils.getHistoryPage(nextPageToken, service, domain, workflowExecution, config);
            DecisionTask task = new DecisionTask();
            List<HistoryEvent> events = history.getEvents();
            events = truncateHistory(events);
            if (events == null) {
                return null;
            }
            task.setEvents(events);
            task.setWorkflowExecution(workflowExecution);
            task.setWorkflowType(executionInfo.getWorkflowType());
            task.setNextPageToken(history.getNextPageToken());
            return task;
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
        protected DecisionTask getNextHistoryTask(String nextPageToken) {
            DecisionTask result = new DecisionTask();
            Iterator<HistoryEvent> iterator = history.iterator();
            if (!iterator.hasNext()) {
                throw new IllegalStateException("empty history");
            }
            HistoryEvent startEvent = iterator.next();
            WorkflowExecutionStartedEventAttributes startedAttributes = startEvent.getWorkflowExecutionStartedEventAttributes();
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
                eventType = EventType.fromValue(event.getEventType());
                events.add(event);
                if (eventType == EventType.DecisionTaskStarted) {
                    previousStartedEventId = startedEventId;
                    startedEventId = event.getEventId();
                    lastStartedIndex = index;
                }
                index++;
            }
            if (events.size() > lastStartedIndex + 1) {
                events = events.subList(0, lastStartedIndex + 1);
            }
            result.setEvents(events);
            result.setPreviousStartedEventId(previousStartedEventId);
            result.setStartedEventId(startedEventId);
            result.setWorkflowExecution(workflowExecution);
            WorkflowType workflowType = startedAttributes.getWorkflowType();
            result.setWorkflowType(workflowType);
            return result;
        }
    }

    private final Iterator<DecisionTask> taskIterator;

    private final AsyncDecisionTaskHandler taskHandler;

    private int replayUpToEventId;

    private final AtomicBoolean replayed = new AtomicBoolean();

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
                            Class<T> workflowImplementationType) throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowImplementationType, null);
    }

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
                            Class<T> workflowImplementationType, SimpleWorkflowClientConfig config) throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowImplementationType, config, null);
    }

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
                            Class<T> workflowImplementationType, SimpleWorkflowClientConfig config, ChildWorkflowIdHandler childWorkflowIdHandler) throws InstantiationException, IllegalAccessException {
        POJOWorkflowDefinitionFactoryFactory ff = new POJOWorkflowDefinitionFactoryFactory();
        ff.addWorkflowImplementationType(workflowImplementationType);
        taskIterator = new ServiceDecisionTaskIterator(service, domain, workflowExecution, config);
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
                            final T workflowImplementation) throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowImplementation, null);
    }

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
                            final T workflowImplementation, SimpleWorkflowClientConfig config) throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowImplementation, null, null);
    }

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
                            final T workflowImplementation, SimpleWorkflowClientConfig config, ChildWorkflowIdHandler childWorkflowIdHandler) throws InstantiationException, IllegalAccessException {
        WorkflowDefinitionFactoryFactory ff = new WorkflowReplayerPOJOFactoryFactory(workflowImplementation);
        taskIterator = new ServiceDecisionTaskIterator(service, domain, workflowExecution, config);
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
                            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory)
            throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowDefinitionFactoryFactory, null);
    }

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
                            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory, SimpleWorkflowClientConfig config)
            throws InstantiationException, IllegalAccessException {
        this(service, domain, workflowExecution, workflowDefinitionFactoryFactory, config, null);
    }

    public WorkflowReplayer(AmazonSimpleWorkflow service, String domain, WorkflowExecution workflowExecution,
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

    public WorkflowReplayer(Iterator<DecisionTask> decisionTasks, Class<T> workflowImplementationType)
            throws InstantiationException, IllegalAccessException {
        this(decisionTasks, workflowImplementationType, null);
    }

    public WorkflowReplayer(Iterator<DecisionTask> decisionTasks, Class<T> workflowImplementationType, ChildWorkflowIdHandler childWorkflowIdHandler)
            throws InstantiationException, IllegalAccessException {
        POJOWorkflowDefinitionFactoryFactory ff = new POJOWorkflowDefinitionFactoryFactory();
        ff.addWorkflowImplementationType(workflowImplementationType);
        taskIterator = decisionTasks;
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(Iterator<DecisionTask> decisionTasks, final T workflowImplementation)
            throws InstantiationException, IllegalAccessException {
        this(decisionTasks, workflowImplementation, null);
    }

    public WorkflowReplayer(Iterator<DecisionTask> decisionTasks, final T workflowImplementation, ChildWorkflowIdHandler childWorkflowIdHandler)
            throws InstantiationException, IllegalAccessException {
        WorkflowDefinitionFactoryFactory ff = new WorkflowReplayerPOJOFactoryFactory(workflowImplementation);
        taskIterator = decisionTasks;
        taskHandler = new AsyncDecisionTaskHandler(ff, childWorkflowIdHandler);
    }

    public WorkflowReplayer(Iterator<DecisionTask> decisionTasks,
                            WorkflowDefinitionFactoryFactory workflowDefinitionFactoryFactory)
            throws InstantiationException, IllegalAccessException {
        this(decisionTasks, workflowDefinitionFactoryFactory, null);
    }

    public WorkflowReplayer(Iterator<DecisionTask> decisionTasks,
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
        return taskHandler.handleDecisionTask(taskIterator);
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
