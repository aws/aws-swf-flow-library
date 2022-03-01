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
package com.amazonaws.services.simpleworkflow.flow;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.pojo.POJOWorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.worker.GenericWorkflowWorker;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;

public class WorkflowWorker implements WorkerBase {

    private final GenericWorkflowWorker genericWorker;

    private final POJOWorkflowDefinitionFactoryFactory factoryFactory ;

    private final Collection<Class<?>> workflowImplementationTypes = new ArrayList<Class<?>>();

    public WorkflowWorker(AmazonSimpleWorkflow service, String domain, String taskListToPoll) {
        this(new GenericWorkflowWorker(service, domain, taskListToPoll));
    }

    public WorkflowWorker(AmazonSimpleWorkflow service, String domain, String taskListToPoll, SimpleWorkflowClientConfig config) {
        this(new GenericWorkflowWorker(service, domain, taskListToPoll, config));
    }

    public WorkflowWorker(GenericWorkflowWorker genericWorker) {
        Objects.requireNonNull(genericWorker,"the workflow worker is required");
        this.genericWorker = genericWorker;
        this.factoryFactory =  new POJOWorkflowDefinitionFactoryFactory();
        this.genericWorker.setWorkflowDefinitionFactoryFactory(factoryFactory);
    }

    @Override
    public SimpleWorkflowClientConfig getClientConfig() {
        return genericWorker.getClientConfig();
    }

    @Override
    public AmazonSimpleWorkflow getService() {
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
    public void setRegisterDomain(boolean registerDomain) {
        genericWorker.setRegisterDomain(registerDomain);
    }

    @Override
    public long getDomainRetentionPeriodInDays() {
        return genericWorker.getDomainRetentionPeriodInDays();
    }

    @Override
    public void setDomainRetentionPeriodInDays(long domainRetentionPeriodInDays) {
        genericWorker.setDomainRetentionPeriodInDays(domainRetentionPeriodInDays);
    }

    @Override
    public String getTaskListToPoll() {
        return genericWorker.getTaskListToPoll();
    }

    @Override
    public double getMaximumPollRatePerSecond() {
        return genericWorker.getMaximumPollRatePerSecond();
    }

    /**
     * Maximum rate of polling and executing decisions (as they are done by the
     * same thread synchronously) by this WorkflowWorker.
     */
    @Override
    public void setMaximumPollRatePerSecond(double maximumPollRatePerSecond) {
        genericWorker.setMaximumPollRatePerSecond(maximumPollRatePerSecond);
    }

    @Override
    public int getMaximumPollRateIntervalMilliseconds() {
        return genericWorker.getMaximumPollRateIntervalMilliseconds();
    }

    /**
     * Time interval used to measure the polling rate. For example if
     * {@link #setMaximumPollRatePerSecond(double)} is 100 and interval is 1000
     * milliseconds then if first 100 requests take 10 milliseconds polling is
     * suspended for 990 milliseconds. If poll interval is changed to 100
     * milliseconds then polling is suspended for 90 milliseconds.
     */
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

    public void setChildWorkflowIdHandler(ChildWorkflowIdHandler childWorkflowIdHandler) {
        genericWorker.setChildWorkflowIdHandler(childWorkflowIdHandler);
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
    public void shutdownNow() {
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

    public void setWorkflowImplementationTypes(Collection<Class<?>> workflowImplementationTypes)
            throws InstantiationException, IllegalAccessException {
        for (Class<?> workflowImplementationType : workflowImplementationTypes) {
            addWorkflowImplementationType(workflowImplementationType);
        }
    }

    public Collection<Class<?>> getWorkflowImplementationTypes() {
        return workflowImplementationTypes;
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType) throws InstantiationException, IllegalAccessException {
        factoryFactory.addWorkflowImplementationType(workflowImplementationType);
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType, DataConverter converter) throws InstantiationException, IllegalAccessException {
        factoryFactory.addWorkflowImplementationType(workflowImplementationType, converter);
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType, DataConverter converter, Object[] constructorArgs) throws InstantiationException, IllegalAccessException {
        factoryFactory.addWorkflowImplementationType(workflowImplementationType, converter, constructorArgs, null);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[genericWorker=" + genericWorker + ", wokflowImplementationTypes="
                + workflowImplementationTypes + "]";
    }

    @Override
    public boolean shutdownAndAwaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return genericWorker.shutdownAndAwaitTermination(timeout, unit);
    }

    public DataConverter getDataConverter() {
        return factoryFactory.getDataConverter();
    }

    public void setDefaultConverter(DataConverter converter) {
        factoryFactory.setDataConverter(converter);
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType,
                                              Map<String, Integer> maximumAllowedComponentImplementationVersions)
            throws InstantiationException, IllegalAccessException {
        factoryFactory.addWorkflowImplementationType(workflowImplementationType, maximumAllowedComponentImplementationVersions);
    }

    @Override
    public void setDisableTypeRegistrationOnStart(boolean disableTypeRegistrationOnStart) {
        genericWorker.setDisableTypeRegistrationOnStart(disableTypeRegistrationOnStart);
    }

    @Override
    public boolean isDisableTypeRegistrationOnStart() {
        return genericWorker.isDisableTypeRegistrationOnStart();
    }

    public void addWorkflowImplementationType(Class<?> workflowImplementationType, DataConverter converter,
                                              Map<String, Integer> maximumAllowedComponentImplementationVersions)
            throws InstantiationException, IllegalAccessException {
        factoryFactory.addWorkflowImplementationType(workflowImplementationType, converter,
                null, maximumAllowedComponentImplementationVersions);
    }

    /**
     * @see WorkflowComponentImplementationVersion
     * @param maximumAllowedImplementationVersions
     *            {key->WorkflowType, value->{key->componentName,
     *            value->maximumAllowedVersion}}
     */
    public void setMaximumAllowedComponentImplementationVersions(
            Map<WorkflowType, Map<String, Integer>> maximumAllowedImplementationVersions) {
        factoryFactory.setMaximumAllowedComponentImplementationVersions(maximumAllowedImplementationVersions);
    }

    public Map<WorkflowType, Map<String, Integer>> getMaximumAllowedComponentImplementationVersions() {
        return factoryFactory.getMaximumAllowedComponentImplementationVersions();
    }

}
