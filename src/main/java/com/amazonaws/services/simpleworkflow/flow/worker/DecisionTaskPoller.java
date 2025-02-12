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

import static com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper.BROKEN_PIPE_ERROR_PREDICATE;
import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution.fromSdkType;
import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowType.fromSdkType;

import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecutionMetadata;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricHelper;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.Metrics;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.NullMetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskRequest;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.RespondDecisionTaskCompletedRequest;
import software.amazon.awssdk.services.swf.model.TaskList;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 */
public class DecisionTaskPoller implements TaskPoller<DecisionTaskPoller.DecisionTaskIterator> {

    private static final Log log = LogFactory.getLog(DecisionTaskPoller.class);

    private static final Log decisionsLog = LogFactory.getLog(DecisionTaskPoller.class.getName() + ".decisions");

    protected class DecisionTaskIterator implements Iterator<PollForDecisionTaskResponse> {

        @Getter
        private PollForDecisionTaskResponse firstDecisionTask;

        private PollForDecisionTaskResponse next;

        private final MetricsContext metricsContext;

        public DecisionTaskIterator(final Metrics metrics) {
            this.metricsContext = new MetricsContext(metrics);
            next = firstDecisionTask = metrics.recordSupplier(() -> pollInternal(null), MetricName.Operation.POLL_FOR_DECISION_TASK.getName(), TimeUnit.MILLISECONDS);
            if (hasNext()) {
                this.metricsContext.incrementPageCount();
                this.metricsContext.setMaxEventId(next.startedEventId(),
                    fromSdkType(next.workflowType()));
                MetricHelper.recordMetrics(next, metrics);
            }
        }

        @Override
        public boolean hasNext() {
            final boolean hasNext = next != null;
            if (!hasNext) {
                metricsContext.close();
            }
            return hasNext;
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
                try {
                    next = metricsContext.getMetrics()
                        .recordSupplier(() -> pollInternal(next.nextPageToken()), MetricName.Operation.POLL_FOR_DECISION_TASK.getName(),
                            TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    // Error is used as it fails a decision instead of affecting workflow code
                    throw new Error("Failure getting next page of history events.", e);
                }

                if (next != null) {
                    metricsContext.incrementPageCount();
                }

                // Just to not keep around the history page
                if (firstDecisionTask != result) {
                    firstDecisionTask = firstDecisionTask.toBuilder().events((HistoryEvent) null).build();
                }
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private PollForDecisionTaskResponse pollInternal(final String nextToken) {
            try {
                return poll(nextToken);
            } catch (Exception e) {
                metricsContext.close();
                throw e;
            }
        }

        @Getter
        private class MetricsContext {
            private final Metrics metrics;

            private final AtomicBoolean isClosed = new AtomicBoolean(false);

            private int pages;

            private long maxEventId;

            private WorkflowType workflowType;

            public MetricsContext(final Metrics metrics) {
                this.metrics = metrics;
                pages = 0;
                maxEventId = 0;
                this.metrics.addProperty(MetricName.Property.TASK_LIST.getName(), taskListToPoll);
                this.metrics.addProperty(MetricName.Property.DOMAIN.getName(), domain);
            }

            public void incrementPageCount() {
                if (!isClosed.get()) {
                    pages++;
                }
            }

            public void setMaxEventId(final long maxEventId, final WorkflowType workflowType) {
                if (!isClosed.get()) {
                    this.maxEventId = maxEventId;
                    this.workflowType = workflowType;
                }
            }

            public void close() {
                if (isClosed.compareAndSet(false, true)) {
                    if (pages > 0) {
                        metricsContext.getMetrics().recordCount(MetricName.PAGE_COUNT.getName(), getPages(), MetricName.getOperationDimension(MetricName.Operation.POLL_FOR_DECISION_TASK.getName()));
                    }
                    metricsContext.getMetrics().recordCount(MetricName.EMPTY_POLL_COUNT.getName(), getPages() == 0, MetricName.getOperationDimension(MetricName.Operation.POLL_FOR_DECISION_TASK.getName()));
                    if (maxEventId > 0) {
                        metricsContext.getMetrics().recordCount(MetricName.MAXIMUM_HISTORY_EVENT_ID.getName(), maxEventId, MetricName.getWorkflowTypeDimension(workflowType));
                    }
                    metrics.close();
                }
            }
        }
    }

    @Getter
    private SwfClient service;

    @Getter
    @Setter
    private String domain;

    @Getter
    @Setter
    private String taskListToPoll;

    @Getter
    private String identity;

    private boolean validated;

    @Getter
    private DecisionTaskHandler decisionTaskHandler;

    private boolean suspended;

    @Setter
    private boolean shutdownRequested;

    private final Lock lock = new ReentrantLock();

    private final Condition suspentionCondition = lock.newCondition();

    @Getter
    @Setter
    private SimpleWorkflowClientConfig config;

    @Getter
    @Setter
    private MetricsRegistry metricsRegistry;

    public DecisionTaskPoller() {
        identity = ManagementFactory.getRuntimeMXBean().getName();
        metricsRegistry = new NullMetricsRegistry();
    }

    public DecisionTaskPoller(SwfClient service, String domain, String taskListToPoll,
        DecisionTaskHandler decisionTaskHandler) {
        this(service, domain, taskListToPoll, decisionTaskHandler, null);
    }

    public DecisionTaskPoller(SwfClient service, String domain, String taskListToPoll,
        DecisionTaskHandler decisionTaskHandler, SimpleWorkflowClientConfig config) {
        this();
        this.service = service;
        this.domain = domain;
        this.taskListToPoll = taskListToPoll;
        this.decisionTaskHandler = decisionTaskHandler;
        this.config = config;
    }

    public void setIdentity(String identity) {
        validated = false;
        this.identity = identity;
    }

    public void setDecisionTaskHandler(DecisionTaskHandler decisionTaskHandler) {
        validated = false;
        this.decisionTaskHandler = decisionTaskHandler;
    }

    public void setService(SwfClient service) {
        validated = false;
        this.service = service;
    }

    /**
     * Poll for a decision task
     *
     * @return null if poll timed out
     */
    private PollForDecisionTaskResponse poll(String nextResultToken) {
        validate();
        PollForDecisionTaskRequest.Builder pollRequestBuilder = PollForDecisionTaskRequest.builder()
            .domain(domain).identity(identity).nextPageToken(nextResultToken);

        pollRequestBuilder.taskList(TaskList.builder().name(taskListToPoll).build());
        // Set startAtPreviousStartedEvent to true for pollers of an affinity workflow worker
        if (decisionTaskHandler.getAffinityHelper() != null) {
            pollRequestBuilder.startAtPreviousStartedEvent(decisionTaskHandler.getAffinityHelper().isAffinityWorker());
        }

        PollForDecisionTaskRequest pollRequest = pollRequestBuilder.build();

        if (log.isDebugEnabled()) {
            log.debug("poll request begin: " + pollRequest);
        }

        pollRequest = RequestTimeoutHelper.overridePollRequestTimeout(pollRequest, config);
        PollForDecisionTaskResponse result = service.pollForDecisionTask(pollRequest);
        if (log.isDebugEnabled() && result != null) {
            log.debug("poll request returned decision task: workflowType=" + result.workflowType() + ", workflowExecution="
                    + result.workflowExecution() + ", startedEventId=" + result.startedEventId()
                    + ", previousStartedEventId=" + result.previousStartedEventId());
        }

        if (result == null || result.taskToken() == null) {
            result = null;
        }
        return result;
    }

    @Override
    public DecisionTaskIterator poll() throws InterruptedException {
        waitIfSuspended();
        DecisionTaskIterator tasks = new DecisionTaskIterator(metricsRegistry.newMetrics(MetricName.Operation.DECISION_TASK_POLL.getName()));
        if (!tasks.hasNext()) {
            return null;
        }
        return tasks;
    }

    @Override
    public void execute(final DecisionTaskIterator tasks) throws Exception {
        RespondDecisionTaskCompletedRequest taskCompletedRequest = null;
        final Metrics metrics = metricsRegistry.newMetrics(MetricName.Operation.EXECUTE_DECISION_TASK.getName());
        final PollForDecisionTaskResponse firstTask = tasks.getFirstDecisionTask();
        ThreadLocalMetrics.setCurrent(metrics);
        MetricHelper.recordMetrics(firstTask, metrics);
        boolean decisionSubmitted = false;
        boolean establishAffinity = shouldEstablishAffinity(firstTask);
        try {
            HandleDecisionTaskResults handleDecisionTaskResults = decisionTaskHandler.handleDecisionTask(tasks);
            taskCompletedRequest = handleDecisionTaskResults.getRespondDecisionTaskCompletedRequest();
            if (decisionsLog.isTraceEnabled()) {
                decisionsLog.trace(WorkflowExecutionUtils.prettyPrintDecisions(taskCompletedRequest.decisions()));
            }
            taskCompletedRequest = RequestTimeoutHelper.overrideDataPlaneRequestTimeout(taskCompletedRequest, config);

            RespondDecisionTaskCompletedRequest.Builder taskCompletedRequestBuilder = taskCompletedRequest.toBuilder();
            if (establishAffinity) {
                if (!shutdownRequested) {
                    // Override the task list to affinity task list
                    taskCompletedRequestBuilder.taskList(TaskList.builder().name(decisionTaskHandler.getAffinityHelper().getAffinityTaskList()).build());
                    taskCompletedRequestBuilder.taskListScheduleToStartTimeout(String.valueOf(config.getDeciderAffinityConfig().getAffinityTaskListScheduleToStartTimeout().getSeconds()));
                    AsyncDecider decider = handleDecisionTaskResults.getAsyncDecider();
                    if (decider.hasCompletedWithoutUnhandledDecision()) {
                        // Evict decider from cache upon completion
                        config.getDeciderAffinityConfig().getDeciderCache()
                            .remove(fromSdkType(firstTask.workflowExecution()));
                    } else {
                        // Cache the decider after clearing its history events
                        decider.getHistoryHelper().clearHistoryEvents();
                        config.getDeciderAffinityConfig().getDeciderCache()
                            .put(fromSdkType(firstTask.workflowExecution()), decider);
                    }
                } else {
                    // Clear the host specific TaskList override if shutdown is initiated to ensure newer decision tasks get scheduled on original TaskList
                    taskCompletedRequestBuilder.taskList(TaskList.builder().name(handleDecisionTaskResults.getAsyncDecider().getOriginalTaskList()).build());
                }
            }
            taskCompletedRequest = taskCompletedRequestBuilder.build();
            final RespondDecisionTaskCompletedRequest request = taskCompletedRequest;
            metrics.recordRunnable(() -> service.respondDecisionTaskCompleted(request), MetricName.Operation.RESPOND_DECISION_TASK_COMPLETED.getName(), TimeUnit.MILLISECONDS);
            decisionSubmitted = true;
            forceFetchFullHistoryIfNeeded(establishAffinity, firstTask, metrics);
        } catch (Error e) {
            if (log.isWarnEnabled()) {
                log.warn("DecisionTask failure: taskId= " + firstTask.startedEventId() + ", workflowExecution="
                    + firstTask.workflowExecution(), e);
            }
            throw e;
        } catch (Exception e) {
            if (log.isWarnEnabled()) {
                log.warn("DecisionTask failure: taskId= " + firstTask.startedEventId() + ", workflowExecution="
                    + firstTask.workflowExecution(), e);
            }
            if (log.isDebugEnabled() && firstTask.events() != null) {
                log.debug("Failed taskId=" + firstTask.startedEventId() + " history: "
                    + WorkflowExecutionUtils.prettyPrintHistory(firstTask.events(), true));
            }
            if (taskCompletedRequest != null && decisionsLog.isWarnEnabled()) {
                decisionsLog.warn("Failed taskId=" + firstTask.startedEventId() + " decisions="
                    + WorkflowExecutionUtils.prettyPrintDecisions(taskCompletedRequest.decisions()));
            }

            if (e instanceof SdkClientException && BROKEN_PIPE_ERROR_PREDICATE.test((SdkClientException) e)) {
                log.error("Unable to submit Decisions because request may have exceeded allowed maximum request size");
                metrics.recordCount(MetricName.REQUEST_SIZE_MAY_BE_EXCEEDED.getName(), 1);
            }

            throw e;
        } finally {
            if (establishAffinity && !decisionSubmitted) {
                config.getDeciderAffinityConfig().getDeciderCache()
                    .remove(fromSdkType(firstTask.workflowExecution()));
            }
            if (taskCompletedRequest != null && taskCompletedRequest.decisions() != null) {
                ThreadLocalMetrics.getMetrics().recordCount(MetricName.DECISION_COUNT.getName(), taskCompletedRequest.decisions().size(), MetricName.getResultDimension(decisionSubmitted));
            }
            metrics.recordCount(MetricName.DROPPED_TASK.getName(), !decisionSubmitted,
                MetricName.getWorkflowTypeDimension(fromSdkType(firstTask.workflowType())));
            metrics.close();
            ThreadLocalMetrics.clearCurrent();
        }
    }

    private boolean shouldEstablishAffinity(PollForDecisionTaskResponse decisionTask) {
        boolean deciderAffinityEnabled = config != null && config.getDeciderAffinityConfig() != null
                && decisionTaskHandler.getAffinityHelper().getAffinityTaskList() != null;
        if (!deciderAffinityEnabled) {
            return false;
        }
        boolean establishAffinityPredicateResult = true;
        HistoryEvent firstEvent = decisionTask.events().get(0);
        if (EventType.WORKFLOW_EXECUTION_STARTED.equals(EventType.fromValue(firstEvent.eventTypeAsString()))) {
            establishAffinityPredicateResult = config.getDeciderAffinityConfig().getEstablishAffinityPredicate().test(
                WorkflowExecutionMetadata.fromSdkType(firstEvent.workflowExecutionStartedEventAttributes()));
        }
        return establishAffinityPredicateResult;
    }

    private void forceFetchFullHistoryIfNeeded(boolean isAffinityEstablished, PollForDecisionTaskResponse decisionTask, Metrics metrics) {
        // For a workflow execution with multiple pages of history events, 30% of time we'd fetch its full history if decider affinity is enabled.
        // This is to keep stable traffic across SWF internal components so SWF can handle cold start of workflow applications with decider affinity enabled.
        if (isAffinityEstablished) {
            int fullHistoryForceFetched = 0;
            int fullHistoryForceFetchFailure = 0;
            try {
                if (decisionTaskHandler.getAffinityHelper().shouldForceFetchFullHistory(decisionTask)) {
                    WorkflowHistoryDecisionTaskIterator historyIterator = decisionTaskHandler.getAffinityHelper().getHistoryIterator(decisionTask);
                    while (historyIterator.hasNext()) {
                        historyIterator.next();
                    }
                    fullHistoryForceFetched = 1;
                }
            } catch (Throwable throwable) {
                // If the underlying GetWorkflowExecutionHistory API fails, we emit a metric without rethrowing so it won't fail the decision task
                if (log.isWarnEnabled()) {
                    log.warn("Fetching full history failure: taskId= " + decisionTask.startedEventId() + ", workflowExecution="
                            + decisionTask.workflowExecution(), throwable);
                }
                fullHistoryForceFetchFailure = 1;
            }
            metrics.recordCount(MetricName.AFFINITY_FULL_HISTORY_FORCE_FETCHED.getName(), fullHistoryForceFetched,
                MetricName.getWorkflowTypeDimension(fromSdkType(decisionTask.workflowType())));
            metrics.recordCount(MetricName.AFFINITY_FULL_HISTORY_FORCE_FETCH_FAILURE.getName(), fullHistoryForceFetchFailure,
                MetricName.getWorkflowTypeDimension(fromSdkType(decisionTask.workflowType())));
        }
    }

    private void waitIfSuspended() throws InterruptedException {
        lock.lock();
        try {
            while (suspended) {
                suspentionCondition.await();
            }
        }
        finally {
            lock.unlock();
        }
    }

    private void validate() throws IllegalStateException {
        if (validated) {
            return;
        }
        checkFieldSet("decisionTaskHandler", decisionTaskHandler);
        checkFieldSet("service", service);
        checkFieldSet("identity", identity);
        validated = true;
    }

    private void checkFieldSet(String fieldName, Object fieldValue) throws IllegalStateException {
        if (fieldValue == null) {
            throw new IllegalStateException("Required field " + fieldName + " is not set");
        }
    }

    @Override
    public void suspend() {
        lock.lock();
        try {
            suspended = true;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void resume() {
        lock.lock();
        try {
            suspended = false;
            suspentionCondition.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isSuspended() {
        lock.lock();
        try {
            return suspended;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
        shutdownRequested = true;
    }

    @Override
    public SuspendableSemaphore getPollingSemaphore() {
        // No polling semaphore for DecisionTaskPoller
        return null;
    }
}
