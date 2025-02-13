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

import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.swf.model.DecisionTaskTimeoutType;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;

public class HistoryHelper {

    private static final Log historyLog = LogFactory.getLog(HistoryHelper.class.getName() + ".history");

    @RequiredArgsConstructor
    @Getter
    private class SingleDecisionData {

        private final List<HistoryEvent> decisionEvents;
        private final long replayCurrentTimeMilliseconds;
        private final String workflowContextData;
    }
    
    class SingleDecisionEventsIterator implements Iterator<SingleDecisionData> {

        private EventsIterator events;

        @Getter
        private ComponentVersions componentVersions = new ComponentVersions();

        private String workflowContextData;

        private SingleDecisionData current;

        private SingleDecisionData next;

        public SingleDecisionEventsIterator(Iterator<PollForDecisionTaskResponse> decisionTasks) {
            events = new EventsIterator(decisionTasks);
            fillNext();
            current = next;
            fillNext();
        }
 
        @Override
        public boolean hasNext() {
            return current != null;
        }
 
        @Override
        public SingleDecisionData next() {
            SingleDecisionData result = current;
            current = next;
            fillNext();
            return result;
        }
        
        /**
         * Load events for the next decision. This method has to deal with the
         * following edge cases to support correct replay:
         * 
         * <li>DecisionTaskTimedOut. The events for the next decision are the
         * ones from the currently processed decision DecisionTaskStarted event
         * to the next DecisionTaskStarted that is not followed by
         * DecisionTaskTimedOut.
         * 
         * <li>Events that were added to a history during a decision. These
         * events appear between DecisionTaskStarted and DecisionTaskCompleted
         * events. They should be processed after all events that correspond to
         * decisions that follow DecisionTaskCompletedEvent.
         */
        private void fillNext() {
            boolean decisionTaskTimedOut = false;
            List<HistoryEvent> decisionStartToCompletionEvents = new ArrayList<>();
            List<HistoryEvent> decisionCompletionToStartEvents = new ArrayList<>();
            boolean concurrentToDecision = true;
            int lastDecisionIndex = -1;
            long nextReplayCurrentTimeMilliseconds = -1;
            while (events.hasNext()) {
                HistoryEvent event = events.next();
                EventType eventType = EventType.fromValue(event.eventTypeAsString());
                if (eventType == EventType.DECISION_TASK_COMPLETED) {
                    String executionContext = event.decisionTaskCompletedEventAttributes().executionContext();
                    updateWorkflowContextDataAndComponentVersions(executionContext);
                    concurrentToDecision = false;
                }
                else if (eventType == EventType.DECISION_TASK_STARTED) {
                    nextReplayCurrentTimeMilliseconds = event.eventTimestamp().toEpochMilli();
                    if (decisionTaskTimedOut) {
                        current.getDecisionEvents().addAll(decisionStartToCompletionEvents);
                        decisionStartToCompletionEvents = new ArrayList<>();
                        decisionTaskTimedOut = false;
                    }
                    else {
                        break;
                    }
                }
                else if (eventType.equals(EventType.DECISION_TASK_TIMED_OUT)) {
                    DecisionTaskTimeoutType timeoutType = DecisionTaskTimeoutType.valueOf(event.decisionTaskTimedOutEventAttributes().timeoutTypeAsString());
                    // Ignore DecisionTaskTimedOut events caused by schedule-to-start timeout,
                    // as they don't have corresponding DecisionTaskStarted events.
                    if (timeoutType == DecisionTaskTimeoutType.START_TO_CLOSE) {
                        decisionTaskTimedOut = true;
                    }
                }
                else if (eventType == EventType.DECISION_TASK_SCHEDULED) {
                    // skip
                }
                else if (eventType == EventType.MARKER_RECORDED) {
                    // ignore
                }
                else if (eventType == EventType.RECORD_MARKER_FAILED) {
                    // ignore
                }
                else {
                    if (concurrentToDecision) {
                        decisionStartToCompletionEvents.add(event);
                    }
                    else {
                        if (isDecisionEvent(eventType)) {
                            lastDecisionIndex = decisionCompletionToStartEvents.size();
                        }
                        decisionCompletionToStartEvents.add(event);
                    }
                }
            }
            List<HistoryEvent> nextEvents = reorderEvents(decisionStartToCompletionEvents, decisionCompletionToStartEvents, lastDecisionIndex);
            next = new SingleDecisionData(nextEvents, nextReplayCurrentTimeMilliseconds, workflowContextData);
        }
 
        public PollForDecisionTaskResponse getDecisionTask() {
            return events.getDecisionTask();
        }
 
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
 
        private boolean isDecisionEvent(EventType eventType) {
            switch (eventType) {
            case ACTIVITY_TASK_SCHEDULED:
            case SCHEDULE_ACTIVITY_TASK_FAILED:
            case ACTIVITY_TASK_CANCEL_REQUESTED:
            case REQUEST_CANCEL_ACTIVITY_TASK_FAILED:
            case MARKER_RECORDED:
            case RECORD_MARKER_FAILED:
            case WORKFLOW_EXECUTION_COMPLETED:
            case COMPLETE_WORKFLOW_EXECUTION_FAILED:
            case WORKFLOW_EXECUTION_FAILED:
            case FAIL_WORKFLOW_EXECUTION_FAILED:
            case WORKFLOW_EXECUTION_CANCELED:
            case CANCEL_WORKFLOW_EXECUTION_FAILED:
            case WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
            case CONTINUE_AS_NEW_WORKFLOW_EXECUTION_FAILED:
            case TIMER_STARTED:
            case START_TIMER_FAILED:
            case TIMER_CANCELED:
            case CANCEL_TIMER_FAILED:
            case SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
            case SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
            case REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
            case REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
            case START_CHILD_WORKFLOW_EXECUTION_INITIATED:
            case START_CHILD_WORKFLOW_EXECUTION_FAILED:
                return true;
            default:
                return false;
            }
        }
 
        /**
         * Reorder events to correspond to the order in which decider sees them.
         * The difference is that events that were added during decision task
         * execution should be processed after events that correspond to the
         * decisions. Otherwise, the replay is going to break.
         */
        private List<HistoryEvent> reorderEvents(List<HistoryEvent> decisionStartToCompletionEvents,
                List<HistoryEvent> decisionCompletionToStartEvents, int lastDecisionIndex) {
            List<HistoryEvent> reordered;
            int size = decisionStartToCompletionEvents.size() + decisionCompletionToStartEvents.size();
 
            reordered = new ArrayList<>(size);
            // First are events that correspond to the previous task decisions
            if (lastDecisionIndex >= 0) {
                reordered.addAll(decisionCompletionToStartEvents.subList(0, lastDecisionIndex + 1));
            }
            // Second are events that were added during previous task execution
            reordered.addAll(decisionStartToCompletionEvents);
            // The last are events that were added after previous task completion
            if (decisionCompletionToStartEvents.size() > lastDecisionIndex + 1) {
                reordered.addAll(decisionCompletionToStartEvents.subList(lastDecisionIndex + 1,
                        decisionCompletionToStartEvents.size()));
            }
            return reordered;
        }
 
        // Should be in sync with GenericWorkflowClientExternalImpl.getWorkflowState
        private void updateWorkflowContextDataAndComponentVersions(String executionContext) {
            if (executionContext != null && executionContext.startsWith(AsyncDecisionTaskHandler.COMPONENT_VERSION_MARKER)) {
                Scanner scanner = new Scanner(executionContext);
                scanner.useDelimiter(AsyncDecisionTaskHandler.COMPONENT_VERSION_SEPARATORS_PATTERN);
                scanner.next();
                int size = scanner.nextInt();
                for (int i = 0; i < size; i++) {
                    String componentName = scanner.next();
                    int version = scanner.nextInt();
                    if (componentName == null) {
                        throw new IncompatibleWorkflowDefinition("null component name in line " + i
                                + " in the execution context: " + executionContext);
                    }
                    componentVersions.setVersionFromHistory(componentName, version);
                }
                workflowContextData = scanner.next(".*");
            }
            else {
                workflowContextData = executionContext;
            }
        }
    }

    class EventsIterator implements Iterator<HistoryEvent> {

        private final FailureTracker failureTracker = new FailureTracker();

        private Iterator<PollForDecisionTaskResponse> decisionTasks;

        @Getter
        private PollForDecisionTaskResponse decisionTask;

        @Getter
        private List<HistoryEvent> events;

        private int index;

        public EventsIterator(Iterator<PollForDecisionTaskResponse> decisionTasks) {
            this.decisionTasks = decisionTasks;
            if (decisionTasks.hasNext()) {
                decisionTask = decisionTasks.next();
                events = decisionTask.events();
                if (historyLog.isTraceEnabled()) {
                    historyLog.trace(WorkflowExecutionUtils.prettyPrintHistory(events, true));
                }
            }
            else {
                decisionTask = null;
            }
        }

        @Override
        public boolean hasNext() {
            final boolean hasNext = decisionTask != null && (index < events.size() || decisionTasks.hasNext());
            if (!hasNext) {
                failureTracker.recordMetric();
            }
            return hasNext;
        }

        @Override
        public HistoryEvent next() {
            if (index == events.size()) {
                decisionTask = decisionTasks.next();
                events = decisionTask.events();
                if (historyLog.isTraceEnabled()) {
                    historyLog.trace(WorkflowExecutionUtils.prettyPrintHistory(events, true));
                }
                index = 0;
            }
            final HistoryEvent historyEvent = events.get(index++);
            failureTracker.processEvent(historyEvent);
            return historyEvent;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        void clearDecisionTasksAndHistoryEvents() {
            decisionTasks = null;
            events = null;
            decisionTask = decisionTask.toBuilder().events((HistoryEvent) null).build();
        }

        /**
         * Counts successive DecisionTaskTimedOut events. To avoid counting same failure event more than once during
         * replay, we only record metrics for the most recent Decision Task events.
         */
        private class FailureTracker {
            private int successiveDecisionFailureCount = 0;

            private boolean metricAlreadyRecorded = false;

            public void processEvent(HistoryEvent historyEvent) {
                switch (EventType.fromValue(historyEvent.eventTypeAsString())) {
                    case DECISION_TASK_COMPLETED:
                        resetCount();
                        break;
                    case DECISION_TASK_TIMED_OUT:
                        successiveDecisionFailureCount++;
                        break;
                    default:
                        break;
                }
            }

            public int getFailureCount() {
                return successiveDecisionFailureCount;
            }

            public void recordMetric() {
                if (!metricAlreadyRecorded) {
                    ThreadLocalMetrics.getMetrics().recordCount(MetricName.SUCCESSIVE_DECISION_FAILURE.getName(), failureTracker.getFailureCount());
                    metricAlreadyRecorded = true;
                }
            }

            private void resetCount() {
                successiveDecisionFailureCount = 0;
            }
        }
    }
    
    private final SingleDecisionEventsIterator singleDecisionEventsIterator;
    
    private SingleDecisionData currentDecisionData;

    void setComponentVersions(ComponentVersions componentVersions) {
        singleDecisionEventsIterator.componentVersions = componentVersions;
    }

    void setWorkflowContextData(String workflowContextData) {
        singleDecisionEventsIterator.workflowContextData = workflowContextData;
    }

    void clearHistoryEvents() {
        singleDecisionEventsIterator.events.clearDecisionTasksAndHistoryEvents();
        singleDecisionEventsIterator.current = null;
        singleDecisionEventsIterator.next = null;
    }

    public HistoryHelper(Iterator<PollForDecisionTaskResponse> decisionTasks) {
        this.singleDecisionEventsIterator = new SingleDecisionEventsIterator(decisionTasks);
    }

    public List<HistoryEvent> getSingleDecisionEvents() {
        if (!singleDecisionEventsIterator.hasNext()) {
            currentDecisionData = null;
            return null;
        }
        currentDecisionData = singleDecisionEventsIterator.next();
        return currentDecisionData.getDecisionEvents();
    }

    public PollForDecisionTaskResponse getDecisionTask() {
        return singleDecisionEventsIterator.getDecisionTask();
    }
    
    public String getWorkflowContextData() {
        if (currentDecisionData == null) {
            throw new IllegalStateException();
        }
        return currentDecisionData.getWorkflowContextData();
    }
 
    public long getReplayCurrentTimeMilliseconds() {
        if (currentDecisionData == null) {
            throw new IllegalStateException();
        }
        return currentDecisionData.getReplayCurrentTimeMilliseconds();
    }
 
    public ComponentVersions getComponentVersions() {
        return singleDecisionEventsIterator.getComponentVersions();
    }

    public long getLastNonReplayEventId() {
        Long result = getDecisionTask().previousStartedEventId();
        if (result == null) {
            return 0;
        }
        return result;
    }
}
