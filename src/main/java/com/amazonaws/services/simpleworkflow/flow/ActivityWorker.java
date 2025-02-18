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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementation;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricsRegistry;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOActivityImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.worker.GenericActivityWorker;
import software.amazon.awssdk.services.swf.SwfClient;

public class ActivityWorker implements WorkerBase {

    private final GenericActivityWorker genericWorker;

    private final POJOActivityImplementationFactory factory;

    public ActivityWorker(SwfClient service, String domain, String taskListToPoll) {
        this(service, domain, taskListToPoll, null);
    }

    public ActivityWorker(SwfClient service, String domain, String taskListToPoll, SimpleWorkflowClientConfig config) {
        this(new GenericActivityWorker(service, domain, taskListToPoll, config));
    }

    public ActivityWorker(GenericActivityWorker genericWorker) {
        Objects.requireNonNull(genericWorker,"the activity worker is required");
        this.genericWorker = genericWorker;
        this.factory =  new POJOActivityImplementationFactory();
        genericWorker.setActivityImplementationFactory(factory);
    }

    public void setActivitiesImplementations(Iterable<Object> activitiesImplementations)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        factory.setActivitiesImplementations(activitiesImplementations);
    }

    public Iterable<Object> getActivitiesImplementations() {
        return factory.getActivitiesImplementations();
    }

    public List<ActivityType> addActivitiesImplementations(Iterable<Object> activitiesImplementations)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        return factory.addActivitiesImplementations(activitiesImplementations);
    }

    public List<ActivityType> addActivitiesImplementations(Iterable<Object> activitiesImplementations, DataConverter dataConverter)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        return factory.addActivitiesImplementations(activitiesImplementations, dataConverter);
    }

    public List<ActivityType> addActivitiesImplementation(Object activitiesImplementation)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        return factory.addActivitiesImplementation(activitiesImplementation);
    }

    public List<ActivityType> addActivitiesImplementation(Object activitiesImplementation, DataConverter converter)
            throws InstantiationException, IllegalAccessException, SecurityException, NoSuchMethodException {
        return factory.addActivitiesImplementation(activitiesImplementation, converter);
    }

    public Iterable<ActivityType> getActivityTypesToRegister() {
        return factory.getActivityTypesToRegister();
    }

    public ActivityImplementation getActivityImplementation(ActivityType activityType) {
        return factory.getActivityImplementation(activityType);
    }

    public DataConverter getDataConverter() {
        return factory.getDataConverter();
    }

    public void setDataConverter(DataConverter dataConverter) {
        factory.setDataConverter(dataConverter);
    }

    @Override
    public boolean shutdownAndAwaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.shutdownAndAwaitTermination(timeout, unit);
    }

    @Override
    public void shutdownNow() {
        genericWorker.shutdownNow();
    }

    @Override
    public SimpleWorkflowClientConfig getClientConfig() {
        return genericWorker.getClientConfig();
    }

    @Override
    public SwfClient getService() {
        return genericWorker.getService();
    }

    @Override
    public String getDomain() {
        return genericWorker.getDomain();
    }

    @Override
    public boolean isRegisterDomain() {
        return genericWorker.isRegisterDomain();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.awaitTermination(timeout, unit);
    }

    @Override
    public boolean gracefulShutdown(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.gracefulShutdown(timeout, unit);
    }

    @Override
    public void setRegisterDomain(boolean registerDomain) {
        genericWorker.setRegisterDomain(registerDomain);
    }

    @Override
    public long getDomainRetentionPeriodInDays() {
        return genericWorker.getDomainRetentionPeriodInDays();
    }

    @Override
    public void setDomainRetentionPeriodInDays(long days) {
        genericWorker.setDomainRetentionPeriodInDays(days);
    }

    @Override
    public String getTaskListToPoll() {
        return genericWorker.getTaskListToPoll();
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

    @Override
    public void setDisableTypeRegistrationOnStart(boolean disableTypeRegistrationOnStart) {
        genericWorker.setDisableTypeRegistrationOnStart(disableTypeRegistrationOnStart);
    }

    @Override
    public boolean isDisableTypeRegistrationOnStart() {
        return genericWorker.isDisableTypeRegistrationOnStart();
    }
    
    @Override
    public void registerTypesToPoll() {
        genericWorker.registerTypesToPoll();
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
    public boolean isRunning() {
        return genericWorker.isRunning();
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
    public String toString() {
        return this.getClass().getSimpleName() + "[genericWorker=" + genericWorker + ", factory=" + factory + "]";
    }

}
