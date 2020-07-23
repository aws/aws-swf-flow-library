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

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.WorkerBase;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.model.DomainAlreadyExistsException;
import com.amazonaws.services.simpleworkflow.model.RegisterDomainRequest;

public abstract class GenericWorker<T> implements WorkerBase {

    class ExecutorThreadFactory implements ThreadFactory {

        private AtomicInteger threadIndex = new AtomicInteger();

        private final String threadPrefix;

        public ExecutorThreadFactory(String threadPrefix) {
            this.threadPrefix = threadPrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread result = new Thread(r);
            result.setName(threadPrefix + (threadIndex.incrementAndGet()));
            result.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            return result;
        }
    }

    private class PollingTask implements Runnable {
        private final TaskPoller<T> poller;

        PollingTask(final TaskPoller poller) {
            this.poller = poller;
        }

        @Override
        public void run() {
            try {
                while(true) {
                    log.debug("poll task begin");
                    if (pollingExecutor.isShutdown() || workerExecutor.isShutdown()) {
                        return;
                    }
                    final int availableWorkers = workerExecutor.getMaximumPoolSize() - workerExecutor.getActiveCount();
                    if (availableWorkers < 1) {
                        log.debug("no available workers");
                        return;
                    }
                    pollBackoffThrottler.throttle();
                    if (pollingExecutor.isShutdown() || workerExecutor.isShutdown()) {
                        return;
                    }
                    if (pollRateThrottler != null) {
                        pollRateThrottler.throttle();
                    }
                    if (pollingExecutor.isShutdown() || workerExecutor.isShutdown()) {
                        return;
                    }
                    T task = poller.poll();
                    if (task == null) {
                        log.debug("no work returned");
                        return;
                    }
                    workerExecutor.execute(new ExecuteTask(poller, task));
                    log.debug("poll task end");
                }
            } catch (final Throwable e) {
                pollBackoffThrottler.failure();
                if (!(e.getCause() instanceof InterruptedException)) {
                    uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
                }
            }
        }
    }

    private class ExecuteTask implements Runnable {
        private final TaskPoller<T> poller;
        private final T task;

        ExecuteTask(final TaskPoller poller, final T task) {
            this.poller = poller;
            this.task = task;
        }

        @Override
        public void run() {
            try {
                log.debug("execute task begin");
                poller.execute(task);
                pollBackoffThrottler.success();
                log.debug("execute task end");
            } catch (Throwable e) {
                pollBackoffThrottler.failure();
                if (!(e.getCause() instanceof InterruptedException)) {
                    uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
                }
            }
        }
    }

    private static final Log log = LogFactory.getLog(GenericWorker.class);

    protected static final int MAX_IDENTITY_LENGTH = 256;

    protected AmazonSimpleWorkflow service;

    protected String domain;

    protected boolean registerDomain;

    protected long domainRetentionPeriodInDays = FlowConstants.NONE;

    private String taskListToPoll;

    private int maximumPollRateIntervalMilliseconds = 1000;

    private double maximumPollRatePerSecond;

    private double pollBackoffCoefficient = 2;

    private long pollBackoffInitialInterval = 100;

    private long pollBackoffMaximumInterval = 60000;

    private boolean disableTypeRegistrationOnStart;

    private boolean disableServiceShutdownOnStop;

    private boolean suspended;

    private final AtomicBoolean startRequested = new AtomicBoolean();

    private final AtomicBoolean shutdownRequested = new AtomicBoolean();

    private ScheduledExecutorService pollingExecutor;

    private ThreadPoolExecutor workerExecutor;

    private String identity = ManagementFactory.getRuntimeMXBean().getName();

    private int pollThreadCount = 1;

    private int executeThreadCount = 1;

    private BackoffThrottler pollBackoffThrottler;

    private Throttler pollRateThrottler;

    protected UncaughtExceptionHandler uncaughtExceptionHandler = new UncaughtExceptionHandler() {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error("Failure in thread " + t.getName(), e);
        }
    };

    private TaskPoller poller;

    public GenericWorker(AmazonSimpleWorkflow service, String domain, String taskListToPoll) {
        this.service = service;
        this.domain = domain;
        this.taskListToPoll = taskListToPoll;
    }

    public GenericWorker() {
        identity = ManagementFactory.getRuntimeMXBean().getName();
        int length = Math.min(identity.length(), GenericWorker.MAX_IDENTITY_LENGTH);
        identity = identity.substring(0, length);
    }

    @Override
    public AmazonSimpleWorkflow getService() {
        return service;
    }

    public void setService(AmazonSimpleWorkflow service) {
        this.service = service;
    }

    @Override
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    @Override
    public boolean isRegisterDomain() {
        return registerDomain;
    }

    /**
     * Should domain be registered on startup. Default is <code>false</code>.
     * When enabled {@link #setDomainRetentionPeriodInDays(long)} property is
     * required.
     */
    @Override
    public void setRegisterDomain(boolean registerDomain) {
        this.registerDomain = registerDomain;
    }

    @Override
    public long getDomainRetentionPeriodInDays() {
        return domainRetentionPeriodInDays;
    }

    @Override
    public void setDomainRetentionPeriodInDays(long domainRetentionPeriodInDays) {
        this.domainRetentionPeriodInDays = domainRetentionPeriodInDays;
    }

    @Override
    public String getTaskListToPoll() {
        return taskListToPoll;
    }

    public void setTaskListToPoll(String taskListToPoll) {
        this.taskListToPoll = taskListToPoll;
    }

    @Override
    public double getMaximumPollRatePerSecond() {
        return maximumPollRatePerSecond;
    }

    @Override
    public void setMaximumPollRatePerSecond(double maximumPollRatePerSecond) {
        this.maximumPollRatePerSecond = maximumPollRatePerSecond;
    }

    @Override
    public int getMaximumPollRateIntervalMilliseconds() {
        return maximumPollRateIntervalMilliseconds;
    }

    @Override
    public void setMaximumPollRateIntervalMilliseconds(int maximumPollRateIntervalMilliseconds) {
        this.maximumPollRateIntervalMilliseconds = maximumPollRateIntervalMilliseconds;
    }

    @Override
    public UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return uncaughtExceptionHandler;
    }

    @Override
    public void setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }

    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public void setIdentity(String identity) {
        this.identity = identity;
    }

    @Override
    public long getPollBackoffInitialInterval() {
        return pollBackoffInitialInterval;
    }

    @Override
    public void setPollBackoffInitialInterval(long backoffInitialInterval) {
        if (backoffInitialInterval < 0) {
            throw new IllegalArgumentException("expected value should be positive or 0: " + backoffInitialInterval);
        }
        this.pollBackoffInitialInterval = backoffInitialInterval;
    }

    @Override
    public long getPollBackoffMaximumInterval() {
        return pollBackoffMaximumInterval;
    }

    @Override
    public void setPollBackoffMaximumInterval(long backoffMaximumInterval) {
        if (backoffMaximumInterval <= 0) {
            throw new IllegalArgumentException("expected value should be positive: " + backoffMaximumInterval);
        }
        this.pollBackoffMaximumInterval = backoffMaximumInterval;
    }

    /**
     * @see #setDisableServiceShutdownOnStop(boolean)
     */
    @Override
    public boolean isDisableServiceShutdownOnStop() {
        return disableServiceShutdownOnStop;
    }

    /**
     * By default when @{link {@link #shutdown()} or @{link
     * {@link #shutdownNow()} is called the worker calls
     * {@link AmazonSimpleWorkflow#shutdown()} on the service instance it is
     * configured with before shutting down internal thread pools. Otherwise
     * threads that are waiting on a poll request might block shutdown for the
     * duration of a poll. This flag allows disabling this behavior.
     *
     * @param disableServiceShutdownOnStop
     *            <code>true</code> means do not call
     *            {@link AmazonSimpleWorkflow#shutdown()}
     */
    @Override
    public void setDisableServiceShutdownOnStop(boolean disableServiceShutdownOnStop) {
        this.disableServiceShutdownOnStop = disableServiceShutdownOnStop;
    }

    @Override
    public double getPollBackoffCoefficient() {
        return pollBackoffCoefficient;
    }

    @Override
    public void setPollBackoffCoefficient(double backoffCoefficient) {
        if (backoffCoefficient < 1.0) {
            throw new IllegalArgumentException("expected value should be bigger or equal to 1.0: " + backoffCoefficient);
        }
        this.pollBackoffCoefficient = backoffCoefficient;
    }

    @Override
    public int getPollThreadCount() {
        return pollThreadCount;
    }

    @Override
    public void setPollThreadCount(int threadCount) {
        checkStarted();
        this.pollThreadCount = threadCount;
        /*
            It is actually not very useful to have poll thread count
            larger than execute thread count. Since execute thread count
            is a new concept introduced since Flow-3.7, to make the
            major version upgrade more friendly, try to bring the
            execute thread count to match poll thread count if client
            does not the set execute thread count explicitly.
         */
        if (this.executeThreadCount < threadCount) {
            this.executeThreadCount = threadCount;
        }
    }

    @Override
    public int getExecuteThreadCount() {
        return executeThreadCount;
    }

    @Override
    public void setExecuteThreadCount(int threadCount) {
        checkStarted();
        this.executeThreadCount = threadCount;
    }

    @Override
    public void setDisableTypeRegistrationOnStart(boolean disableTypeRegistrationOnStart) {
        this.disableTypeRegistrationOnStart = disableTypeRegistrationOnStart;
    }

    @Override
    public boolean isDisableTypeRegistrationOnStart() {
        return disableTypeRegistrationOnStart;
    }

    @Override
    public void start() {
        if (log.isInfoEnabled()) {
            log.info("start: " + toString());
        }
        if (shutdownRequested.get()) {
            throw new IllegalStateException("Shutdown Requested. Not restartable.");
        }
        if (!startRequested.compareAndSet(false, true)) {
            return;
        }
        checkRequiredProperty(service, "service");
        checkRequiredProperty(domain, "domain");
        checkRequiredProperty(taskListToPoll, "taskListToPoll");
        checkRequiredProperties();

        if (registerDomain) {
            registerDomain();
        }

        if (!disableTypeRegistrationOnStart) {
            registerTypesToPoll();
        }

        if (maximumPollRatePerSecond > 0.0) {
            pollRateThrottler = new Throttler("pollRateThrottler " + taskListToPoll, maximumPollRatePerSecond,
                    maximumPollRateIntervalMilliseconds);
        }
        poller = createPoller();
        if (suspended) {
            poller.suspend();
        }
        pollBackoffThrottler = new BackoffThrottler(pollBackoffInitialInterval, pollBackoffMaximumInterval,
                pollBackoffCoefficient);

        workerExecutor = new ThreadPoolExecutor(executeThreadCount, executeThreadCount, 1, TimeUnit.MINUTES,
                new SynchronousQueue<>(), new BlockCallerPolicy());
        ExecutorThreadFactory pollExecutorThreadFactory = getExecutorThreadFactory("Worker");
        workerExecutor.setThreadFactory(pollExecutorThreadFactory);

        pollingExecutor = new ScheduledThreadPoolExecutor(pollThreadCount, getExecutorThreadFactory("Poller"));
        for (int i = 0; i < pollThreadCount; i++) {
            pollingExecutor.scheduleWithFixedDelay(new PollingTask(poller), 0, 1, TimeUnit.SECONDS);
        }
    }

    private ExecutorThreadFactory getExecutorThreadFactory(final String type) {
        return new ExecutorThreadFactory(getPollThreadNamePrefix() + " " + type);
    }

    protected abstract String getPollThreadNamePrefix();

    protected abstract TaskPoller<T> createPoller();

    protected abstract void checkRequiredProperties();

    private void registerDomain() {
        if (domainRetentionPeriodInDays == FlowConstants.NONE) {
            throw new IllegalStateException("required property domainRetentionPeriodInDays is not set");
        }
        try {
            service.registerDomain(new RegisterDomainRequest().withName(domain).withWorkflowExecutionRetentionPeriodInDays(
                    String.valueOf(domainRetentionPeriodInDays)));
        }
        catch (DomainAlreadyExistsException e) {
            if (log.isTraceEnabled()) {
                log.trace("Domain is already registered: " + domain);
            }
        }
    }

    protected void checkRequiredProperty(Object value, String name) {
        if (value == null) {
            throw new IllegalStateException("required property " + name + " is not set");
        }
    }

    protected void checkStarted() {
        if (isStarted()) {
            throw new IllegalStateException("started");
        }
    }

    private boolean isStarted() {
        return startRequested.get();
    }

    @Override
    public void shutdown() {
        if (log.isInfoEnabled()) {
            log.info("shutdown");
        }
        if (!shutdownRequested.compareAndSet(false, true)) {
            return;
        }
        if (!isStarted()) {
            return;
        }
        if (!disableServiceShutdownOnStop) {
            service.shutdown();
        }
        pollingExecutor.shutdown();
        workerExecutor.shutdown();
    }

    @Override
    public void shutdownNow() {
        if (log.isInfoEnabled()) {
            log.info("shutdownNow");
        }
        if (!shutdownRequested.compareAndSet(false, true)) {
            return;
        }
        if (!isStarted()) {
            return;
        }
        if (!disableServiceShutdownOnStop) {
            service.shutdown();
        }
        pollingExecutor.shutdownNow();
        workerExecutor.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long start = System.currentTimeMillis();
        boolean terminated = pollingExecutor.awaitTermination(timeout, unit);
        long elapsed = System.currentTimeMillis() - start;
        long left = TimeUnit.MILLISECONDS.convert(timeout, unit) - elapsed;
        terminated &= workerExecutor.awaitTermination(left, TimeUnit.MILLISECONDS);
        return terminated;
    }

    /**
     * The graceful shutdown will wait for existing polls and tasks to complete
     * unless the timeout is not enough. It does not shutdown the SWF client.
     */
    @Override
    public boolean gracefulShutdown(long timeout, TimeUnit unit) throws InterruptedException {
        setDisableServiceShutdownOnStop(true);
        return shutdownAndAwaitTermination(timeout, unit);
    }

    /**
     * This method will also shutdown SWF client unless you {@link #setDisableServiceShutdownOnStop(boolean)} to <code>true</code>.
     */
    @Override
    public boolean shutdownAndAwaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutMilliseconds = TimeUnit.MILLISECONDS.convert(timeout, unit);
        long start = System.currentTimeMillis();
        if (shutdownRequested.compareAndSet(false, true)) {
            if (!isStarted()) {
                return true;
            }

            if (!disableServiceShutdownOnStop) {
                service.shutdown();
            }
            pollingExecutor.shutdown();
            pollingExecutor.awaitTermination(timeout, unit);
            workerExecutor.shutdown();
            long elapsed = System.currentTimeMillis() - start;
            long left = timeoutMilliseconds - elapsed;
            workerExecutor.awaitTermination(left, unit);
        }
        long elapsed = System.currentTimeMillis() - start;
        long left = timeoutMilliseconds - elapsed;
        return awaitTermination(left, TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[service=" + service + ", domain=" + domain + ", taskListToPoll="
                + taskListToPoll + ", identity=" + identity + ", backoffInitialInterval=" + pollBackoffInitialInterval
                + ", backoffMaximumInterval=" + pollBackoffMaximumInterval + ", backoffCoefficient=" + pollBackoffCoefficient
                + "]";
    }

    @Override
    public boolean isRunning() {
        return isStarted() && !pollingExecutor.isTerminated() && !workerExecutor.isTerminated();
    }

    @Override
    public void suspendPolling() {
        if (log.isInfoEnabled()) {
            log.info("suspendPolling");
        }
        suspended = true;
        if (poller != null) {
            poller.suspend();
        }
    }

    @Override
    public void resumePolling() {
        if (log.isInfoEnabled()) {
            log.info("resumePolling");
        }
        suspended = false;
        if (poller != null) {
            poller.resume();
        }
    }

    @Override
    public boolean isPollingSuspended() {
        if (poller != null) {
            return poller.isSuspended();
        }
        return suspended;
    }

    @Override
    public void setPollingSuspended(boolean flag) {
        if (flag) {
            suspendPolling();
        }
        else {
            resumePolling();
        }
    }

}
