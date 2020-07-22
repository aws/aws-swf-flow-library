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

import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.PollForDecisionTaskRequest;
import com.amazonaws.services.simpleworkflow.model.RespondDecisionTaskCompletedRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 *
 */
public class DecisionTaskPoller implements TaskPoller<DecisionTaskPoller.DecisionTaskIterator> {

    private static final Log log = LogFactory.getLog(DecisionTaskPoller.class);

    private static final Log decisionsLog = LogFactory.getLog(DecisionTaskPoller.class.getName() + ".decisions");

    protected class DecisionTaskIterator implements Iterator<DecisionTask> {

        private final DecisionTask firstDecisionTask;

        private DecisionTask next;

        public DecisionTaskIterator() {
            next = firstDecisionTask = poll(null);
        }

        @Override
        public boolean hasNext() {
            return next != null;
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
                try {
                    next = poll(next.getNextPageToken());
                }
                catch (Exception e) {
                    // Error is used as it fails a decision instead of affecting workflow code
                    throw new Error("Failure getting next page of history events.", e);
                }

                // Just to not keep around the history page
                if (firstDecisionTask != result) {
                    firstDecisionTask.setEvents(null);
                }
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public DecisionTask getFirstDecisionTask() {
            return firstDecisionTask;
        }

    }

    private AmazonSimpleWorkflow service;

    private String domain;

    private String taskListToPoll;

    private String identity;

    private boolean validated;

    private DecisionTaskHandler decisionTaskHandler;

    private boolean suspended;

    private final Lock lock = new ReentrantLock();

    private final Condition suspentionCondition = lock.newCondition();

    public DecisionTaskPoller() {
        identity = ManagementFactory.getRuntimeMXBean().getName();
    }

    public DecisionTaskPoller(AmazonSimpleWorkflow service, String domain, String taskListToPoll,
                              DecisionTaskHandler decisionTaskHandler) {
        this.service = service;
        this.domain = domain;
        this.taskListToPoll = taskListToPoll;
        this.decisionTaskHandler = decisionTaskHandler;
        identity = ManagementFactory.getRuntimeMXBean().getName();
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        validated = false;
        this.identity = identity;
    }

    public AmazonSimpleWorkflow getService() {
        return service;
    }

    public String getDomain() {
        return domain;
    }

    public DecisionTaskHandler getDecisionTaskHandler() {
        return decisionTaskHandler;
    }

    public void setDecisionTaskHandler(DecisionTaskHandler decisionTaskHandler) {
        validated = false;
        this.decisionTaskHandler = decisionTaskHandler;
    }

    public void setService(AmazonSimpleWorkflow service) {
        validated = false;
        this.service = service;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getTaskListToPoll() {
        return taskListToPoll;
    }

    public void setTaskListToPoll(String pollTaskList) {
        this.taskListToPoll = pollTaskList;
    }

    /**
     * Poll for a task using {@link #getPollTimeoutInSeconds()}
     *
     * @param nextResultToken
     *
     * @return null if poll timed out
     * @throws DeciderExecutorConfigurationException
     */
    private DecisionTask poll(String nextResultToken) {
        validate();
        PollForDecisionTaskRequest pollRequest = new PollForDecisionTaskRequest();

        pollRequest.setDomain(domain);
        pollRequest.setIdentity(identity);
        pollRequest.setNextPageToken(nextResultToken);

        pollRequest.setTaskList(new TaskList().withName(taskListToPoll));

        if (log.isDebugEnabled()) {
            log.debug("poll request begin: " + pollRequest);
        }
        DecisionTask result = service.pollForDecisionTask(pollRequest);
        if (log.isDebugEnabled()) {
            log.debug("poll request returned decision task: workflowType=" + result.getWorkflowType() + ", workflowExecution="
                    + result.getWorkflowExecution() + ", startedEventId=" + result.getStartedEventId()
                    + ", previousStartedEventId=" + result.getPreviousStartedEventId());
        }

        if (result == null || result.getTaskToken() == null) {
            return null;
        }
        return result;
    }

    @Override
    public DecisionTaskIterator poll() throws InterruptedException {
        waitIfSuspended();
        DecisionTaskIterator tasks = new DecisionTaskIterator();
        if (!tasks.hasNext()) {
            return null;
        }
        return tasks;
    }

    @Override
    public void execute(final DecisionTaskIterator tasks) throws Exception {
        RespondDecisionTaskCompletedRequest taskCompletedRequest = null;
        try {
            taskCompletedRequest = decisionTaskHandler.handleDecisionTask(tasks);
            if (decisionsLog.isTraceEnabled()) {
                decisionsLog.trace(WorkflowExecutionUtils.prettyPrintDecisions(taskCompletedRequest.getDecisions()));
            }
            service.respondDecisionTaskCompleted(taskCompletedRequest);
        } catch (Error e) {
            DecisionTask firstTask = tasks.getFirstDecisionTask();
            if (firstTask != null) {
                if (log.isWarnEnabled()) {
                    log.warn("DecisionTask failure: taskId= " + firstTask.getStartedEventId() + ", workflowExecution="
                            + firstTask.getWorkflowExecution(), e);
                }
            }
            throw e;
        } catch (Exception e) {
            DecisionTask firstTask = tasks.getFirstDecisionTask();
            if (firstTask != null) {
                if (log.isWarnEnabled()) {
                    log.warn("DecisionTask failure: taskId= " + firstTask.getStartedEventId() + ", workflowExecution="
                            + firstTask.getWorkflowExecution(), e);
                }
                if (log.isDebugEnabled() && firstTask.getEvents() != null) {
                    log.debug("Failed taskId=" + firstTask.getStartedEventId() + " history: "
                            + WorkflowExecutionUtils.prettyPrintHistory(firstTask.getEvents(), true));
                }
            }
            if (taskCompletedRequest != null && decisionsLog.isWarnEnabled()) {
                decisionsLog.warn("Failed taskId=" + firstTask.getStartedEventId() + " decisions="
                        + WorkflowExecutionUtils.prettyPrintDecisions(taskCompletedRequest.getDecisions()));
            }
            throw e;
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

    /**
     * @param seconds
     * @return
     */

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

    protected void checkFieldNotNegative(String fieldName, long fieldValue) throws IllegalStateException {
        if (fieldValue < 0) {
            throw new IllegalStateException("Field " + fieldName + " is negative");
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
}
