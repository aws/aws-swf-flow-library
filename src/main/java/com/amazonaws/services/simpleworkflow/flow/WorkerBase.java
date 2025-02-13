/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow;

import java.lang.Thread.UncaughtExceptionHandler;

import com.amazonaws.services.simpleworkflow.flow.annotations.SkipTypeRegistration;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.NullMetricsRegistry;
import software.amazon.awssdk.services.swf.SwfClient;

public interface WorkerBase extends SuspendableWorker {

    SwfClient getService();

    SimpleWorkflowClientConfig getClientConfig();

    String getDomain();

    boolean isRegisterDomain();

    /**
     * Should domain be registered on startup. Default is <code>false</code>.
     * When enabled {@link #setDomainRetentionPeriodInDays(long)} property is
     * required.
     * @param registerDomain true if domain should be registered
     */
    void setRegisterDomain(boolean registerDomain);

    long getDomainRetentionPeriodInDays();

    /**
     * Value of DomainRetentionPeriodInDays parameter passed to
     * {@link SwfClient#registerDomain} call. Required when
     * {@link #isRegisterDomain()} is <code>true</code>.
     * @param domainRetentionPeriodInDays Days to retain domain history
     */
    void setDomainRetentionPeriodInDays(long domainRetentionPeriodInDays);

    /**
     * Task list name that given worker polls for tasks.
     * @return Name of task list to poll
     */
    String getTaskListToPoll();

    double getMaximumPollRatePerSecond();

    /**
     * Maximum number of poll request to the task list per second allowed.
     * Default is 0 which means unlimited.
     * @see #setMaximumPollRateIntervalMilliseconds(int)
     *
     * @param maximumPollRatePerSecond - Maximum polls allowed per second
     */
    void setMaximumPollRatePerSecond(double maximumPollRatePerSecond);

    int getMaximumPollRateIntervalMilliseconds();

    /**
     * The sliding window interval used to measure the poll rate. Controls
     * allowed rate burstiness. For example if allowed rate is 10 per second and
     * poll rate interval is 100 milliseconds the poller is not going to allow
     * more then one poll per 100 milliseconds. If poll rate interval is 10
     * seconds then 100 request can be emitted as fast as possible, but then the
     * polling stops until 10 seconds pass.
     *
     * @see #setMaximumPollRatePerSecond(double)
     * @param maximumPollRateIntervalMilliseconds - interval in milliseconds for rate limiting
     */
    void setMaximumPollRateIntervalMilliseconds(int maximumPollRateIntervalMilliseconds);

    UncaughtExceptionHandler getUncaughtExceptionHandler();

    /**
     * Handler notified about poll request and other unexpected failures. The
     * default implementation logs the failures using ERROR level.
     * @param uncaughtExceptionHandler - handler for processing uncaught exceptions
     */
    void setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler);

    String getIdentity();

    /**
     * Set the identity that worker specifies in the poll requests. This value
     * ends up stored in the identity field of the corresponding Start history
     * event. Default is "pid"@"host".
     * 
     * @param identity
     *            maximum size is 256 characters.
     */
    void setIdentity(String identity);

    long getPollBackoffInitialInterval();

    /**
     * Failed poll requests are retried after an interval defined by an
     * exponential backoff algorithm. See BackoffThrottler for more info.
     * 
     * @param backoffInitialInterval
     *            the interval between failure and the first retry. Default is
     *            100.
     */
    void setPollBackoffInitialInterval(long backoffInitialInterval);

    long getPollBackoffMaximumInterval();

    /**
     * @see WorkerBase#setPollBackoffInitialInterval(long)
     * 
     * @param backoffMaximumInterval
     *            maximum interval between poll request retries. Default is
     *            60000 (one minute).
     */
    void setPollBackoffMaximumInterval(long backoffMaximumInterval);

    double getPollBackoffCoefficient();

    /**
     * @see WorkerBase#setPollBackoffInitialInterval(long)
     * 
     * @param backoffCoefficient
     *            coefficient that defines how fast retry interval grows in case
     *            of poll request failures. Default is 2.0.
     */
    void setPollBackoffCoefficient(double backoffCoefficient);

    boolean isDisableServiceShutdownOnStop();

    /**
     * When set to false (which is default) at the beginning of the worker
     * shutdown {@link SwfClient#close()} is called. It causes all
     * outstanding long poll request to disconnect. But also causes all future
     * request (for example activity completions) to SWF fail.
     *
     * @param disableServiceShutdownOnStop - flag to control service shutdown
     */
    void setDisableServiceShutdownOnStop(boolean disableServiceShutdownOnStop);

    int getPollThreadCount();

    /**
     * Defines how many concurrent threads are used by the given worker to poll
     * the specified task list. Default is 1. The size of the task execution
     * thread pool is defined through {@link #setExecuteThreadCount(int)}.
     *
     * @param threadCount - number of concurrent polling threads
     */
    void setPollThreadCount(int threadCount);

    int getExecuteThreadCount();

    /**
     * Defines how many concurrent threads are used by the given worker to poll
     * the specified task list. Default is 100. This will be the actual number of
     * threads which execute tasks which are polled from the specified task list
     * by Poll threads
     *
     * @param threadCount - number of task execution threads
     */
    void setExecuteThreadCount(int threadCount);

    /**
     * Try to register every type (activity or workflow depending on worker)
     * that are configured with the worker.
     * 
     * @see #setDisableTypeRegistrationOnStart(boolean)
     */
    void registerTypesToPoll();

    boolean isRunning();

    /**
     * When set to true disables types registration on start even if
     * {@link SkipTypeRegistration} is not specified. Types still can be
     * registered by calling {@link #registerTypesToPoll()}.
     *
     * @param disableTypeRegistrationOnStart - flag to disable type registration
     */
    void setDisableTypeRegistrationOnStart(boolean disableTypeRegistrationOnStart);

    boolean isDisableTypeRegistrationOnStart();

    /**
     * When set to true allows the task execution threads to terminate
     * if they have been idle for 1 minute.
     *
     * @param allowCoreThreadTimeOut - flag to enable thread timeout
     */
    void setAllowCoreThreadTimeOut(boolean allowCoreThreadTimeOut);

    boolean isAllowCoreThreadTimeOut();

    default MetricsRegistry getMetricsRegistry() {
        return NullMetricsRegistry.getInstance();
    }

    default void setMetricsRegistry(MetricsRegistry factory) {
    }
}
