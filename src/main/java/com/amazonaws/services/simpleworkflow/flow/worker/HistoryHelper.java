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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.EventType;
import com.amazonaws.services.simpleworkflow.model.HistoryEvent;

class HistoryHelper {

    private static final Log historyLog = LogFactory.getLog(HistoryHelper.class.getName() + ".history");

    @RequiredArgsConstructor
    @Getter
    private class SingleDecisionData {

        private final List<HistoryEvent> decisionEvents;
        private final long replayCurrentTimeMilliseconds;
        private final String workflowContextData;
    }

    class SingleDecisionEventsIterator implements Iterator<SingleDecisionData> {

        private final EventsIterator events;

        @Getter
        private final ComponentVersions componentVersions = new ComponentVersions();

        private String workflowContextData;

        private SingleDecisionData current;

        private SingleDecisionData next;

        public SingleDecisionEventsIterator(Iterator<DecisionTask> decisionTasks) {
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
            List<HistoryEvent> decisionStartToCompletionEvents = new ArrayList<HistoryEvent>();
            List<HistoryEvent> decisionCompletionToStartEvents = new ArrayList<HistoryEvent>();
            boolean concurrentToDecision = true;
            int lastDecisionIndex = -1;
            long nextReplayCurrentTimeMilliseconds = -1;
            while (events.hasNext()) {
                HistoryEvent event = events.next();
                EventType eventType = EventType.fromValue(event.getEventType());
                if (eventType == EventType.DecisionTaskCompleted) {
                    String executionContext = event.getDecisionTaskCompletedEventAttributes().getExecutionContext();
                    updateWorkflowContextDataAndComponentVersions(executionContext);
                    concurrentToDecision = false;
                }
                else if (eventType == EventType.DecisionTaskStarted) {
                    nextReplayCurrentTimeMilliseconds = event.getEventTimestamp().getTime();
                    if (decisionTaskTimedOut) {
                        current.getDecisionEvents().addAll(decisionStartToCompletionEvents);
                        decisionStartToCompletionEvents = new ArrayList<HistoryEvent>();
                        decisionTaskTimedOut = false;
                    }
                    else {
                        break;
                    }
                }
                else if (eventType.equals(EventType.DecisionTaskTimedOut)) {
                    decisionTaskTimedOut = true;
                }
                else if (eventType == EventType.DecisionTaskScheduled) {
                    // skip
                }
                else if (eventType == EventType.MarkerRecorded) {
                    // ignore
                }
                else if (eventType == EventType.RecordMarkerFailed) {
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

        public DecisionTask getDecisionTask() {
            return events.getDecisionTask();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private boolean isDecisionEvent(EventType eventType) {
            switch (eventType) {
            case ActivityTaskScheduled:
            case ScheduleActivityTaskFailed:
            case ActivityTaskCancelRequested:
            case RequestCancelActivityTaskFailed:
            case MarkerRecorded:
            case RecordMarkerFailed:
            case WorkflowExecutionCompleted:
            case CompleteWorkflowExecutionFailed:
            case WorkflowExecutionFailed:
            case FailWorkflowExecutionFailed:
            case WorkflowExecutionCanceled:
            case CancelWorkflowExecutionFailed:
            case WorkflowExecutionContinuedAsNew:
            case ContinueAsNewWorkflowExecutionFailed:
            case TimerStarted:
            case StartTimerFailed:
            case TimerCanceled:
            case CancelTimerFailed:
            case SignalExternalWorkflowExecutionInitiated:
            case SignalExternalWorkflowExecutionFailed:
            case RequestCancelExternalWorkflowExecutionInitiated:
            case RequestCancelExternalWorkflowExecutionFailed:
            case StartChildWorkflowExecutionInitiated:
            case StartChildWorkflowExecutionFailed:
                return true;
            default:
                return false;
            }
        }

        /**
         * Reorder events to correspond to the order in which decider sees them.
         * The difference is that events that were added during decision task
         * execution should be processed after events that correspond to the
         * decisions. Otherwise the replay is going to break.
         */
        private List<HistoryEvent> reorderEvents(List<HistoryEvent> decisionStartToCompletionEvents,
                                                 List<HistoryEvent> decisionCompletionToStartEvents, int lastDecisionIndex) {
            List<HistoryEvent> reordered;
            int size = decisionStartToCompletionEvents.size() + decisionStartToCompletionEvents.size();

            reordered = new ArrayList<HistoryEvent>(size);
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

        private final Iterator<DecisionTask> decisionTasks;

        @Getter
        private DecisionTask decisionTask;

        @Getter
        private List<HistoryEvent> events;

        private int index;

        public EventsIterator(Iterator<DecisionTask> decisionTasks) {
            this.decisionTasks = decisionTasks;
            if (decisionTasks.hasNext()) {
                decisionTask = decisionTasks.next();
                events = decisionTask.getEvents();
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
            return decisionTask != null && (index < events.size() || decisionTasks.hasNext());
        }

        @Override
        public HistoryEvent next() {
            if (index == events.size()) {
                decisionTask = decisionTasks.next();
                events = decisionTask.getEvents();
                if (historyLog.isTraceEnabled()) {
                    historyLog.trace(WorkflowExecutionUtils.prettyPrintHistory(events, true));
                }
                index = 0;
            }
            return events.get(index++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private final SingleDecisionEventsIterator singleDecisionEventsIterator;

    private SingleDecisionData currentDecisionData;

    public HistoryHelper(Iterator<DecisionTask> decisionTasks) {
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

    public DecisionTask getDecisionTask() {
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
        Long result = getDecisionTask().getPreviousStartedEventId();
        if (result == null) {
            return 0;
        }
        return result;
    }
}
