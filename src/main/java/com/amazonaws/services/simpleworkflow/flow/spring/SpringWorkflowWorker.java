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
package com.amazonaws.services.simpleworkflow.flow.spring;

import com.amazonaws.services.simpleworkflow.flow.ChildWorkflowIdHandler;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.WorkerBase;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.Metrics;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import com.amazonaws.services.simpleworkflow.flow.worker.GenericWorkflowWorker;
import org.springframework.context.SmartLifecycle;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.swf.SwfClient;

/**
 * To be used with Spring. Assumes that injected implementation bean has
 * "workflow" scope. Otherwise the same object instance will be reused for
 * multiple decisions which is guaranteed to break replay if any instance fields
 * are used.
 */
public class SpringWorkflowWorker implements WorkerBase, SmartLifecycle {

    private final GenericWorkflowWorker genericWorker;

    private final SpringWorkflowDefinitionFactoryFactory factoryFactory;

    private int startPhase;

    protected long terminationTimeoutSeconds = 60;

    private boolean disableAutoStartup;

    public SpringWorkflowWorker() {
        this(new GenericWorkflowWorker());
    }

    public SpringWorkflowWorker(SwfClient service, String domain, String taskListToPoll) {
        this(new GenericWorkflowWorker(service, domain, taskListToPoll));
    }

    public SpringWorkflowWorker(SwfClient service, String domain, String taskListToPoll, SimpleWorkflowClientConfig config) {
        this(new GenericWorkflowWorker(service, domain, taskListToPoll, config));
    }

    public SpringWorkflowWorker(GenericWorkflowWorker genericWorker) {
        Objects.requireNonNull(genericWorker,"the workflow worker is required");
        this.genericWorker = genericWorker;
        this.factoryFactory = new SpringWorkflowDefinitionFactoryFactory();
        this.genericWorker.setWorkflowDefinitionFactoryFactory(factoryFactory);
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

    @Override
    public String getDomain() {
        return genericWorker.getDomain();
    }

    public void setDomain(String domain) {
        genericWorker.setDomain(domain);
    }

    @Override
    public boolean isRegisterDomain() {
        return genericWorker.isRegisterDomain();
    }

    public void setRegisterDomain(boolean registerDomain) {
        genericWorker.setRegisterDomain(registerDomain);
    }

    @Override
    public long getDomainRetentionPeriodInDays() {
        return genericWorker.getDomainRetentionPeriodInDays();
    }

    public void setDomainRetentionPeriodInDays(long domainRetentionPeriodInDays) {
        genericWorker.setDomainRetentionPeriodInDays(domainRetentionPeriodInDays);
    }

    @Override
    public String getTaskListToPoll() {
        return genericWorker.getTaskListToPoll();
    }

    public void setTaskListToPoll(String taskListToPoll) {
        genericWorker.setTaskListToPoll(taskListToPoll);
    }

    public DataConverter getDataConverter() {
        return factoryFactory.getDataConverter();
    }

    public void setDataConverter(DataConverter converter) {
        factoryFactory.setDataConverter(converter);
    }

    @Override
    public double getMaximumPollRatePerSecond() {
        return genericWorker.getMaximumPollRatePerSecond();
    }

    @Override
    public void setMaximumPollRatePerSecond(double maximumPollRatePerSecond) {
        genericWorker.setMaximumPollRatePerSecond(maximumPollRatePerSecond);
    }

    @Override
    public int getMaximumPollRateIntervalMilliseconds() {
        return genericWorker.getMaximumPollRateIntervalMilliseconds();
    }

    @Override
    public void setMaximumPollRateIntervalMilliseconds(int maximumPollRateIntervalMilliseconds) {
        genericWorker.setMaximumPollRateIntervalMilliseconds(maximumPollRateIntervalMilliseconds);
    }

    @Override
    public UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return genericWorker.getUncaughtExceptionHandler();
    }

    @Override
    public void setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
        genericWorker.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    }

    @Override
    public String getIdentity() {
        return genericWorker.getIdentity();
    }

    @Override
   public void setIdentity(String identity) {
        genericWorker.setIdentity(identity);
    }

    @Override
    public long getPollBackoffInitialInterval() {
        return genericWorker.getPollBackoffInitialInterval();
    }

    @Override
    public void setPollBackoffInitialInterval(long backoffInitialInterval) {
        genericWorker.setPollBackoffInitialInterval(backoffInitialInterval);
    }

    @Override
    public long getPollBackoffMaximumInterval() {
        return genericWorker.getPollBackoffMaximumInterval();
    }

    @Override
    public void setPollBackoffMaximumInterval(long backoffMaximumInterval) {
        genericWorker.setPollBackoffMaximumInterval(backoffMaximumInterval);
    }

    @Override
    public boolean isDisableServiceShutdownOnStop() {
        return genericWorker.isDisableServiceShutdownOnStop();
    }

    @Override
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
    public void setDisableTypeRegistrationOnStart(boolean disableTypeRegistrationOnStart) {
        genericWorker.setDisableTypeRegistrationOnStart(disableTypeRegistrationOnStart);
    }

    @Override
    public boolean isDisableTypeRegistrationOnStart() {
        return genericWorker.isDisableTypeRegistrationOnStart();
    }

    @Override
    public double getPollBackoffCoefficient() {
        return genericWorker.getPollBackoffCoefficient();
    }

    @Override
    public void setPollBackoffCoefficient(double backoffCoefficient) {
        genericWorker.setPollBackoffCoefficient(backoffCoefficient);
    }

    @Override
    public int getPollThreadCount() {
        return genericWorker.getPollThreadCount();
    }

    @Override
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

    public void setChildWorkflowIdHandler(ChildWorkflowIdHandler childWorkflowIdHandler) {
        genericWorker.setChildWorkflowIdHandler(childWorkflowIdHandler);
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

    public Iterable<WorkflowType> getWorkflowTypesToRegister() {
        return factoryFactory.getWorkflowTypesToRegister();
    }

    @Override
    public void start() {
        genericWorker.start();
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

    public void stopNow() {
        genericWorker.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.awaitTermination(timeout, unit);
    }

    @Override
    public boolean gracefulShutdown(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.gracefulShutdown(timeout, unit);
    }

    public void setWorkflowImplementations(Iterable<Object> workflowImplementations)
            throws InstantiationException, IllegalAccessException {
        for (Object workflowImplementation : workflowImplementations) {
            addWorkflowImplementation(workflowImplementation);
        }
    }

    public Iterable<Object> getWorkflowImplementations() {
        return factoryFactory.getWorkflowImplementations();
    }

    public void addWorkflowImplementation(Object workflowImplementation) throws InstantiationException, IllegalAccessException {
        factoryFactory.addWorkflowImplementation(workflowImplementation);
    }

    public void setMaximumAllowedComponentImplementationVersions(
            Map<WorkflowType, Map<String, Integer>> maximumAllowedImplementationVersions) {
        factoryFactory.setMaximumAllowedComponentImplementationVersions(maximumAllowedImplementationVersions);
    }

    public Map<WorkflowType, Map<String, Integer>> getMaximumAllowedComponentImplementationVersions() {
        return factoryFactory.getMaximumAllowedComponentImplementationVersions();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[genericWorker=" + genericWorker + ", factoryFactory=" + factoryFactory + "]";
    }

    @Override
    public void stop() {
        final Metrics oldMetrics = ThreadLocalMetrics.getMetrics();
        try (Metrics metrics = getMetricsRegistry().newMetrics(MetricName.Operation.WORKFLOW_WORKER_SHUTDOWN.getName())) {
            ThreadLocalMetrics.setCurrent(metrics);
            gracefulShutdown(terminationTimeoutSeconds, TimeUnit.SECONDS);
            shutdownNow();
        } catch (InterruptedException e) {
        } finally {
            ThreadLocalMetrics.setCurrent(oldMetrics);
        }
    }

    @Override
    public boolean isRunning() {
        return genericWorker.isRunning();
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
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void registerTypesToPoll() {
        genericWorker.registerTypesToPoll();
    }
}
