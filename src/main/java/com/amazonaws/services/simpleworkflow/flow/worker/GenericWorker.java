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

import com.amazonaws.services.simpleworkflow.flow.WorkerBase;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.common.RequestTimeoutHelper;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.NullMetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.DomainAlreadyExistsException;
import software.amazon.awssdk.services.swf.model.LimitExceededException;
import software.amazon.awssdk.services.swf.model.RegisterDomainRequest;

public abstract class GenericWorker<T> implements WorkerBase {

    public enum WorkerType {
        ACTIVITY("Activity"),
        WORKFLOW("Workflow");

        @Getter
        private final String name;

        WorkerType(String name) {
            this.name = name;
        }
    }

    @RequiredArgsConstructor
    class ExecutorThreadFactory implements ThreadFactory {

        private final AtomicInteger threadIndex = new AtomicInteger();

        private final String threadPrefix;

        @Override
        public Thread newThread(Runnable r) {
            Thread result = new Thread(r);
            result.setName(threadPrefix + (threadIndex.incrementAndGet()));
            result.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            return result;
        }
    }

    @RequiredArgsConstructor
    private class PollingTask implements Runnable {

        private final TaskPoller<T> poller;

        @Override
        public void run() {
            try {
                while(true) {
                    log.debug("poll task begin");
                    if (pollingExecutor.isShutdown() || workerExecutor.isShutdown()) {
                        return;
                    }

                    pollBackoffThrottler.throttle();
                    final int availableWorkers = workerExecutor.getMaximumPoolSize() - workerExecutor.getActiveCount();
                    if (availableWorkers < 1) {
                        log.warn("Maximum worker thread capacity reached. Polling will not resume until an existing task completes. Consider increasing the worker thread count.");
                        pollBackoffThrottler.failure();
                        return;
                    }

                    if (pollingExecutor.isShutdown() || workerExecutor.isShutdown()) {
                        return;
                    }
                    if (pollRateThrottler != null) {
                        pollRateThrottler.throttle();
                    }
                    if (pollingExecutor.isShutdown() || workerExecutor.isShutdown()) {
                        return;
                    }
                    boolean semaphoreNeedsRelease = false;
                    SuspendableSemaphore pollingSemaphore = poller.getPollingSemaphore();
                    try {
                        if (pollingSemaphore != null) {
                            pollingSemaphore.acquire();
                        }
                        semaphoreNeedsRelease = true;
                        T task = poller.poll();
                        if (task == null) {
                            log.debug("no work returned");
                            return;
                        }
                        semaphoreNeedsRelease = false;
                        try {
                            workerExecutor.execute(new ExecuteTask(poller, task, pollingSemaphore));
                            log.debug("poll task end");
                        } catch (Exception | Error e) {
                            semaphoreNeedsRelease = true;
                            throw e;
                        }
                    } finally {
                        if (pollingSemaphore != null && semaphoreNeedsRelease) {
                            pollingSemaphore.release();
                        }
                    }
                }
            } catch (final Throwable e) {
                pollBackoffThrottler.failure();
                if (e instanceof LimitExceededException) {
                    log.info("Received LimitExceededException for over-polling on " + taskListToPoll + " TaskList. This is a cue to reduce poll rate on " + taskListToPoll);
                } else if (!(e.getCause() instanceof InterruptedException)) {
                    uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
                }
            }
        }
    }

    @RequiredArgsConstructor
    private class ExecuteTask implements Runnable {
        private final TaskPoller<T> poller;
        private final T task;
        private final SuspendableSemaphore pollingSemaphore;

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
            } finally {
                if (pollingSemaphore != null) {
                    pollingSemaphore.release();
                }
            }
        }
    }

    public static final int DEFAULT_EXECUTOR_THREAD_COUNT = 100;

    private static final Log log = LogFactory.getLog(GenericWorker.class);

    protected static final int MAX_IDENTITY_LENGTH = 256;

    @Getter
    @Setter
    protected SwfClient service;

    @Getter
    @Setter
    protected String domain;

    @Getter
    protected boolean registerDomain;

    @Getter
    @Setter
    protected long domainRetentionPeriodInDays = FlowConstants.NONE;

    @Getter
    @Setter
    private String taskListToPoll;

    @Getter
    @Setter
    private int maximumPollRateIntervalMilliseconds = 1000;

    @Getter
    @Setter
    private double maximumPollRatePerSecond;

    @Getter
    private double pollBackoffCoefficient = 2;

    @Getter
    private long pollBackoffInitialInterval = 100;

    @Getter
    private long pollBackoffMaximumInterval = 60000;

    @Getter
    @Setter
    private boolean disableTypeRegistrationOnStart;

    @Getter
    private boolean disableServiceShutdownOnStop;

    @Getter
    @Setter
    private boolean allowCoreThreadTimeOut;

    private boolean suspended;

    private final AtomicBoolean startRequested = new AtomicBoolean();

    private final AtomicBoolean shutdownRequested = new AtomicBoolean();

    private ScheduledExecutorService pollingExecutor;

    private ThreadPoolExecutor workerExecutor;

    @Getter
    @Setter
    private String identity;

    @Getter
    private int pollThreadCount = 1;

    @Getter
    private int executeThreadCount = DEFAULT_EXECUTOR_THREAD_COUNT;

    private BackoffThrottler pollBackoffThrottler;

    private Throttler pollRateThrottler;

    @Getter
    @Setter
    protected UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> log.error("Failure in thread " + t.getName(), e);

    private TaskPoller<T> poller;

    @Getter
    @Setter
    private SimpleWorkflowClientConfig clientConfig;

    @Getter
    @Setter
    private MetricsRegistry metricsRegistry;

    public GenericWorker(SwfClient service, String domain, String taskListToPoll) {
        this(service, domain, taskListToPoll, null);
    }

    public GenericWorker(SwfClient service, String domain, String taskListToPoll, SimpleWorkflowClientConfig clientConfig) {
        this();
        this.service = service;
        this.domain = domain;
        this.taskListToPoll = taskListToPoll;
        this.clientConfig = clientConfig;
    }

    public GenericWorker() {
        identity = ManagementFactory.getRuntimeMXBean().getName();
        int length = Math.min(identity.length(), GenericWorker.MAX_IDENTITY_LENGTH);
        identity = identity.substring(0, length);
        metricsRegistry = new NullMetricsRegistry();
    }

    GenericWorker(SwfClient service, String domain, String taskListToPoll,
        ScheduledExecutorService pollerExecutor, ThreadPoolExecutor workerExecutor, TaskPoller taskPoller, Boolean startRequested) {
        this(service, domain, taskListToPoll);
        this.pollingExecutor = pollerExecutor;
        this.workerExecutor = workerExecutor;
        this.poller = taskPoller;
        this.startRequested.set(startRequested);
    }

    @Override
    public void start() {
        if (log.isInfoEnabled()) {
            log.info("start: " + this);
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
        workerExecutor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
        ExecutorThreadFactory pollExecutorThreadFactory = getExecutorThreadFactory("Worker");
        workerExecutor.setThreadFactory(pollExecutorThreadFactory);
        metricsRegistry.getExecutorServiceMonitor().monitor(workerExecutor, "com.amazonaws.services.simpleworkflow.flow:type=ThreadPoolState,name=" + getWorkerType().getName() + "Worker");

        pollingExecutor = new ScheduledThreadPoolExecutor(pollThreadCount, getExecutorThreadFactory("Poller"));
        metricsRegistry.getExecutorServiceMonitor().monitor(pollingExecutor, "com.amazonaws.services.simpleworkflow.flow:type=ThreadPoolState,name=" + getWorkerType().getName() + "Poller");
        for (int i = 0; i < pollThreadCount; i++) {
            pollingExecutor.scheduleWithFixedDelay(new PollingTask(poller), 0, 1, TimeUnit.NANOSECONDS);
        }
    }

    private ExecutorThreadFactory getExecutorThreadFactory(final String type) {
        return new ExecutorThreadFactory(getPollThreadNamePrefix() + " " + type);
    }

    protected abstract String getPollThreadNamePrefix();

    protected abstract TaskPoller<T> createPoller();

    protected abstract void checkRequiredProperties();

    protected abstract WorkerType getWorkerType();

    private void registerDomain() {
        if (domainRetentionPeriodInDays == FlowConstants.NONE) {
            throw new IllegalStateException("required property domainRetentionPeriodInDays is not set");
        }
        try {
            RegisterDomainRequest request = RegisterDomainRequest.builder().name(domain)
                .workflowExecutionRetentionPeriodInDays(String.valueOf(domainRetentionPeriodInDays)).build();
            request = RequestTimeoutHelper.overrideControlPlaneRequestTimeout(request, clientConfig);
            service.registerDomain(request);
        } catch (DomainAlreadyExistsException e) {
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
            service.close();
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
            service.close();
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
        final boolean outstandingTasksCompleted = shutdownAndAwaitTermination(timeout, unit);
        ThreadLocalMetrics.getMetrics().recordCount(MetricName.OUTSTANDING_TASKS_DROPPED.getName(), !outstandingTasksCompleted);
        return outstandingTasksCompleted;
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
                service.close();
            }
            poller.shutdown();
            pollingExecutor.shutdown();
            pollingExecutor.awaitTermination(timeoutMilliseconds, TimeUnit.MILLISECONDS);
            workerExecutor.shutdown();
            long elapsed = System.currentTimeMillis() - start;
            long left = timeoutMilliseconds - elapsed;
            workerExecutor.awaitTermination(left, TimeUnit.MILLISECONDS);
        }
        long elapsed = System.currentTimeMillis() - start;
        long left = timeoutMilliseconds - elapsed;
        return awaitTermination(left, TimeUnit.MILLISECONDS);
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
    public void setPollBackoffInitialInterval(long backoffInitialInterval) {
        if (backoffInitialInterval < 0) {
            throw new IllegalArgumentException("expected value should be positive or 0: " + backoffInitialInterval);
        }
        this.pollBackoffInitialInterval = backoffInitialInterval;
    }

    @Override
    public void setPollBackoffMaximumInterval(long backoffMaximumInterval) {
        if (backoffMaximumInterval <= 0) {
            throw new IllegalArgumentException("expected value should be positive: " + backoffMaximumInterval);
        }
        this.pollBackoffMaximumInterval = backoffMaximumInterval;
    }

    /**
     * By default when @{link {@link #shutdown()} or @{link
     * {@link #shutdownNow()} is called the worker calls
     * {@link SwfClient#close()} on the service instance it is
     * configured with before shutting down internal thread pools. Otherwise
     * threads that are waiting on a poll request might block shutdown for the
     * duration of a poll. This flag allows disabling this behavior.
     *
     * @param disableServiceShutdownOnStop
     *            <code>true</code> means do not call
     *            {@link SwfClient#close()}
     */
    @Override
    public void setDisableServiceShutdownOnStop(boolean disableServiceShutdownOnStop) {
        this.disableServiceShutdownOnStop = disableServiceShutdownOnStop;
    }

    @Override
    public void setPollBackoffCoefficient(double backoffCoefficient) {
        if (backoffCoefficient < 1.0) {
            throw new IllegalArgumentException("expected value should be bigger or equal to 1.0: " + backoffCoefficient);
        }
        this.pollBackoffCoefficient = backoffCoefficient;
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
    public void setExecuteThreadCount(int threadCount) {
        checkStarted();
        this.executeThreadCount = threadCount;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[service=" + service + ", domain=" + domain + ", taskListToPoll="
                + taskListToPoll + ", identity=" + identity + ", backoffInitialInterval=" + pollBackoffInitialInterval
                + ", backoffMaximumInterval=" + pollBackoffMaximumInterval + ", backoffCoefficient=" + pollBackoffCoefficient
                + "]";
    }

    protected static ExponentialRetryParameters getRegisterTypeThrottledRetryParameters() {
        ExponentialRetryParameters retryParameters = new ExponentialRetryParameters();
        retryParameters.setBackoffCoefficient(2.0);
        retryParameters.setExpirationInterval(TimeUnit.MINUTES.toMillis(10));
        // Use a large base retry interval since this is built on top of SDK retry.
        retryParameters.setInitialInterval(TimeUnit.SECONDS.toMillis(3));
        retryParameters.setMaximumRetries(29);
        retryParameters.setMaximumRetryInterval(TimeUnit.SECONDS.toMillis(20));
        retryParameters.setMinimumRetries(1);
        return retryParameters;
    }
}
