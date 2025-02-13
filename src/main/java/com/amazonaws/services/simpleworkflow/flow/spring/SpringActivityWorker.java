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
package com.amazonaws.services.simpleworkflow.flow.spring;

import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.WorkerBase;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.Metrics;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOActivityImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.worker.GenericActivityWorker;
import org.springframework.context.SmartLifecycle;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.swf.SwfClient;

public class SpringActivityWorker implements WorkerBase, SmartLifecycle {
    
    private final GenericActivityWorker genericWorker;
    
    private final POJOActivityImplementationFactory factory;

    private int startPhase;

    protected long terminationTimeoutSeconds = 60;

    private boolean disableAutoStartup;
    
    public SpringActivityWorker() {
        this(new GenericActivityWorker());
    }

    public SpringActivityWorker(SwfClient service, String domain, String taskListToPoll) {
        this(new GenericActivityWorker(service, domain, taskListToPoll));
    }

    public SpringActivityWorker(SwfClient service, String domain, String taskListToPoll, SimpleWorkflowClientConfig config) {
        this(new GenericActivityWorker(service, domain, taskListToPoll, config));
    }

    public SpringActivityWorker(GenericActivityWorker genericWorker) {
        Objects.requireNonNull(genericWorker,"the activity worker is required");

        this.genericWorker = genericWorker;
        this.factory =  new POJOActivityImplementationFactory();
        this.genericWorker.setActivityImplementationFactory(factory);
    }

    public SimpleWorkflowClientConfig getClientConfig() {
        return genericWorker.getClientConfig();
    }

    public SwfClient getService() {
        return genericWorker.getService();
    }

    public void setService(SwfClient service) {
        genericWorker.setService(service);
    }
    
    public String getDomain() {
        return genericWorker.getDomain();
    }
    
    public void setDomain(String domain) {
        genericWorker.setDomain(domain);
    }
    
    public boolean isRegisterDomain() {
        return genericWorker.isRegisterDomain();
    }
    
    public void setRegisterDomain(boolean registerDomain) {
        genericWorker.setRegisterDomain(registerDomain);
    }
    
    public long getDomainRetentionPeriodInDays() {
        return genericWorker.getDomainRetentionPeriodInDays();
    }
    
    public void setDomainRetentionPeriodInDays(long domainRetentionPeriodInDays) {
        genericWorker.setDomainRetentionPeriodInDays(domainRetentionPeriodInDays);
    }
    
    public String getTaskListToPoll() {
        return genericWorker.getTaskListToPoll();
    }
    
    public void setTaskListToPoll(String taskListToPoll) {
        genericWorker.setTaskListToPoll(taskListToPoll);
    }
    
    public DataConverter getDataConverter() {
        return factory.getDataConverter();
    }

    public void setDataConverter(DataConverter dataConverter) {
        factory.setDataConverter(dataConverter);
    }

    public double getMaximumPollRatePerSecond() {
        return genericWorker.getMaximumPollRatePerSecond();
    }
    
    public void setMaximumPollRatePerSecond(double maximumPollRatePerSecond) {
        genericWorker.setMaximumPollRatePerSecond(maximumPollRatePerSecond);
    }

    public int getMaximumPollRateIntervalMilliseconds() {
        return genericWorker.getMaximumPollRateIntervalMilliseconds();
    }
    
    public void setMaximumPollRateIntervalMilliseconds(int maximumPollRateIntervalMilliseconds) {
        genericWorker.setMaximumPollRateIntervalMilliseconds(maximumPollRateIntervalMilliseconds);
    }

    public String getIdentity() {
        return genericWorker.getIdentity();
    }
    
    public void setIdentity(String identity) {
        genericWorker.setIdentity(identity);
    }
    
    public UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return genericWorker.getUncaughtExceptionHandler();
    }

    public void setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
        genericWorker.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    }
    
    public long getPollBackoffInitialInterval() {
        return genericWorker.getPollBackoffInitialInterval();
    }
    
    public void setPollBackoffInitialInterval(long backoffInitialInterval) {
        genericWorker.setPollBackoffInitialInterval(backoffInitialInterval);
    }

    public long getPollBackoffMaximumInterval() {
        return genericWorker.getPollBackoffMaximumInterval();
    }

    public void setPollBackoffMaximumInterval(long backoffMaximumInterval) {
        genericWorker.setPollBackoffMaximumInterval(backoffMaximumInterval);
    }

    public double getPollBackoffCoefficient() {
        return genericWorker.getPollBackoffCoefficient();
    }
    
    public void setPollBackoffCoefficient(double backoffCoefficient) {
        genericWorker.setPollBackoffCoefficient(backoffCoefficient);
    }
    
    public int getPollThreadCount() {
        return genericWorker.getPollThreadCount();
    }

    public void setPollThreadCount(int threadCount) {
        genericWorker.setPollThreadCount(threadCount);
    }

    @Override
    public int getExecuteThreadCount() {
        return genericWorker.getExecuteThreadCount();
    }

    @Override
    public void setExecuteThreadCount(int threadCount) {
        genericWorker.setExecuteThreadCount(threadCount);
    }
    
    public boolean isDisableServiceShutdownOnStop() {
        return genericWorker.isDisableServiceShutdownOnStop();
    }
    
    public void setDisableServiceShutdownOnStop(boolean disableServiceShutdownOnStop) {
        genericWorker.setDisableServiceShutdownOnStop(disableServiceShutdownOnStop);
    }

    @Override
    public boolean isAllowCoreThreadTimeOut() {
        return genericWorker.isAllowCoreThreadTimeOut();
    }

    @Override
    public MetricsRegistry getMetricsRegistry() {
        return genericWorker.getMetricsRegistry();
    }

    @Override
    public void setMetricsRegistry(MetricsRegistry factory) {
        genericWorker.setMetricsRegistry(factory);
    }

    @Override
    public void setAllowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) {
        genericWorker.setAllowCoreThreadTimeOut(allowCoreThreadTimeOut);
    }
    
    @Override
    public void suspendPolling() {
        genericWorker.suspendPolling();
    }

    @Override
    public void resumePolling() {
        genericWorker.resumePolling();
    }
    
    @Override
    public boolean isPollingSuspended() {
        return genericWorker.isPollingSuspended();
    }

    @Override
    public void setPollingSuspended(boolean flag) {
        genericWorker.setPollingSuspended(flag);
    }

    @Override
    public void start() {
        genericWorker.start();
    }
    
    public void stopNow() {
        genericWorker.shutdownNow();
    }
    
    @Override
    public void shutdown() {
        genericWorker.shutdown();
    }

    @Override
    public void shutdownNow() {
        genericWorker.shutdownNow();
    }

    @Override
    public boolean shutdownAndAwaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.shutdownAndAwaitTermination(timeout, unit);
    }

    public void shutdownAndAwaitTermination() throws InterruptedException {
        shutdownAndAwaitTermination(terminationTimeoutSeconds, TimeUnit.SECONDS);
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.awaitTermination(timeout, unit);
    }

    @Override
    public boolean gracefulShutdown(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.gracefulShutdown(timeout, unit);
    }

    @Override
    public void stop() {
        final Metrics oldMetrics = ThreadLocalMetrics.getMetrics();
        try (Metrics metrics = getMetricsRegistry().newMetrics(MetricName.Operation.ACTIVITY_WORKER_SHUTDOWN.getName())) {
            ThreadLocalMetrics.setCurrent(metrics);
            gracefulShutdown(terminationTimeoutSeconds, TimeUnit.SECONDS);
            shutdownNow();
        } catch (InterruptedException e) {
        } finally {
            ThreadLocalMetrics.setCurrent(oldMetrics);
        }
    }
    
    public boolean isRunning() {
        return genericWorker.isRunning();
    }
    
    public void setActivitiesImplementations(Iterable<Object> activitiesImplementations)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        for (Object activitiesImplementation : activitiesImplementations) {
            addActivitiesImplementation(activitiesImplementation);
        }
    }
    
    public Iterable<Object> getActivitiesImplementations() {
        return factory.getActivitiesImplementations();
    }
    
    public List<ActivityType> addActivitiesImplementation(Object activitiesImplementation)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        return factory.addActivitiesImplementation(activitiesImplementation);
    }

    @Override
    public void registerTypesToPoll() {
        genericWorker.registerTypesToPoll();
    }

    /**
     * @return default is 0
     */
    @Override
    public int getPhase() {
        return startPhase;
    }

    public void setPhase(int startPhase) {
        this.startPhase = startPhase;
    }
    
    @Override
    public boolean isAutoStartup() {
        return !disableAutoStartup;
    }

    public long getTerminationTimeoutSeconds() {
        return terminationTimeoutSeconds;
    }

    public void setTerminationTimeoutSeconds(long terminationTimeoutSeconds) {
        this.terminationTimeoutSeconds = terminationTimeoutSeconds;
    }

    public boolean isDisableAutoStartup() {
        return disableAutoStartup;
    }

    public void setDisableAutoStartup(boolean disableAutoStartup) {
        this.disableAutoStartup = disableAutoStartup;
    }
    
    @Override
    public void setDisableTypeRegistrationOnStart(boolean disableTypeRegistrationOnStart) {
        genericWorker.setDisableTypeRegistrationOnStart(disableTypeRegistrationOnStart);
    }

    @Override
    public boolean isDisableTypeRegistrationOnStart() {
        return genericWorker.isDisableTypeRegistrationOnStart();
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[genericWorker=" + genericWorker + ", factory=" + factory + "]";
    }
}
