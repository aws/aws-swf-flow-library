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

import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowType.fromSdkType;

import com.amazonaws.services.simpleworkflow.flow.ChildWorkflowIdHandler;
import com.amazonaws.services.simpleworkflow.flow.DefaultChildWorkflowIdHandler;
import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.core.AsyncTaskInfo;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeComponentImplementationVersion;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeImplementationOptions;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Getter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.swf.model.Decision;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.RespondDecisionTaskCompletedRequest;

/**
 * This class is for internal use only and may be changed or removed without prior notice.
 *
 */
public class AsyncDecisionTaskHandler extends DecisionTaskHandler {
	
	static final String COMPONENT_VERSION_MARKER = "*component_version*";
	 
    static final String COMPONENT_VERSION_RECORD_SEPARATOR = "\n";
 
    static final String COMPONENT_VERSION_SEPARATOR = "\t";
 
    static final String COMPONENT_VERSION_SEPARATORS_PATTERN = COMPONENT_VERSION_RECORD_SEPARATOR + "|"
            + COMPONENT_VERSION_SEPARATOR;

    private static final Log log = LogFactory.getLog(AsyncDecisionTaskHandler.class);

    private static final Log asyncThreadDumpLog = LogFactory.getLog(AsyncDecisionTaskHandler.class.getName()
            + ".waitingTasksStacks");

    private final WorkflowDefinitionFactoryFactory definitionFactoryFactory;

    private final ChildWorkflowIdHandler childWorkflowIdHandler;

    private final boolean skipFailedCheck;

    private SimpleWorkflowClientConfig clientConfig = SimpleWorkflowClientConfig.ofDefaults();

    @Getter
    private AffinityHelper affinityHelper;

    public AsyncDecisionTaskHandler(WorkflowDefinitionFactoryFactory definitionFactoryFactory) {
        this(definitionFactoryFactory, false);
    }

    public AsyncDecisionTaskHandler(WorkflowDefinitionFactoryFactory definitionFactoryFactory, ChildWorkflowIdHandler childWorkflowIdHandler) {
        this(definitionFactoryFactory, false, childWorkflowIdHandler);
    }

    public AsyncDecisionTaskHandler(WorkflowDefinitionFactoryFactory definitionFactoryFactory, ChildWorkflowIdHandler childWorkflowIdHandler, SimpleWorkflowClientConfig config) {
        this(definitionFactoryFactory, childWorkflowIdHandler);
        clientConfig = config;
    }

    public AsyncDecisionTaskHandler(WorkflowDefinitionFactoryFactory definitionFactoryFactory, ChildWorkflowIdHandler childWorkflowIdHandler, AffinityHelper affinityHelper, SimpleWorkflowClientConfig config) {
        this(definitionFactoryFactory, childWorkflowIdHandler, config);
        this.affinityHelper = affinityHelper;
    }

    public AsyncDecisionTaskHandler(WorkflowDefinitionFactoryFactory definitionFactoryFactory, boolean skipFailedCheck) {
        this(definitionFactoryFactory, skipFailedCheck, null);
    }

    public AsyncDecisionTaskHandler(WorkflowDefinitionFactoryFactory definitionFactoryFactory, boolean skipFailedCheck, ChildWorkflowIdHandler childWorkflowIdHandler) {
        this.definitionFactoryFactory = definitionFactoryFactory;
        this.skipFailedCheck = skipFailedCheck;
        this.childWorkflowIdHandler = childWorkflowIdHandler != null ? childWorkflowIdHandler : new DefaultChildWorkflowIdHandler();
    }

    @Override
    public HandleDecisionTaskResults handleDecisionTask(Iterator<PollForDecisionTaskResponse> decisionTaskIterator) throws Exception {
        HistoryHelper historyHelper = new HistoryHelper(decisionTaskIterator);
        AsyncDecider decider = createDecider(historyHelper);
        decider.decide();
        // The workflow context data of history helper might be updated when making decisions, so
        // let's get the latest history helper.
        historyHelper = decider.getHistoryHelper();
        DecisionsHelper decisionsHelper = decider.getDecisionsHelper();
        Collection<Decision> decisions = decisionsHelper.getDecisions();
        PollForDecisionTaskResponse decisionTask = historyHelper.getDecisionTask();
        if (log.isDebugEnabled()) {
            log.debug("WorkflowTask taskId=" + decisionTask.startedEventId() + ", taskToken=" + decisionTask.taskToken()
                    + " completed with " + decisions.size() + " new decisions");
        }
        if (decisions.size() == 0 && asyncThreadDumpLog.isTraceEnabled()) {
            asyncThreadDumpLog.trace("Empty decision list with the following waiting tasks:\n"
                    + decider.getAsynchronousThreadDumpAsString());
        }
        RespondDecisionTaskCompletedRequest.Builder completedRequestBuilder = RespondDecisionTaskCompletedRequest.builder();
        completedRequestBuilder.taskToken(decisionTask.taskToken()).decisions(decisions);
        String contextData = decisionsHelper.getWorkflowContextDataToReturn();
        ComponentVersions componentVersions = historyHelper.getComponentVersions();
        Map<String, Integer> versionsToSave = componentVersions.getVersionsToSave();
        String executionContext = getExecutionContext(contextData, versionsToSave);
        if (historyHelper.getWorkflowContextData() == null || !historyHelper.getWorkflowContextData().equals(executionContext)) {
            completedRequestBuilder.executionContext(executionContext);
        }
        return new HandleDecisionTaskResults(completedRequestBuilder.build(), decider);
    }
    
    private String getExecutionContext(String contextData, Map<String, Integer> componentVersions) {
        int versionsSize = componentVersions.size();
        if (versionsSize == 0 && contextData == null) {
            return null;
        }
        StringBuilder executionContext = new StringBuilder();
        if (versionsSize > 0) {
            executionContext.append(COMPONENT_VERSION_MARKER);
            executionContext.append(COMPONENT_VERSION_SEPARATOR);
            executionContext.append(versionsSize);
            executionContext.append(COMPONENT_VERSION_RECORD_SEPARATOR);
            for (Entry<String, Integer> version : componentVersions.entrySet()) {
                executionContext.append(version.getKey());
                executionContext.append(COMPONENT_VERSION_SEPARATOR);
                executionContext.append(version.getValue());
                executionContext.append(COMPONENT_VERSION_RECORD_SEPARATOR);
            }
        }
        executionContext.append(contextData);
        return executionContext.toString();
    }

    @Override
    public WorkflowDefinition loadWorkflowThroughReplay(Iterator<PollForDecisionTaskResponse> decisionTaskIterator) throws Exception {
        HistoryHelper historyHelper = new HistoryHelper(decisionTaskIterator);
        AsyncDecider decider = createDecider(historyHelper);
        decider.decide();
        DecisionsHelper decisionsHelper = decider.getDecisionsHelper();
        if(!skipFailedCheck) {
            if (decisionsHelper.isWorkflowFailed()) {
                throw new IllegalStateException("Cannot load failed workflow", decisionsHelper.getWorkflowFailureCause());
            }
        }
        return decider.getWorkflowDefinition();
    }

    @Override
    public List<AsyncTaskInfo> getAsynchronousThreadDump(Iterator<PollForDecisionTaskResponse> decisionTaskIterator) throws Exception {
        HistoryHelper historyHelper = new HistoryHelper(decisionTaskIterator);
        AsyncDecider decider = createDecider(historyHelper);
        decider.decide();
        return decider.getAsynchronousThreadDump();
    }

    @Override
    public String getAsynchronousThreadDumpAsString(Iterator<PollForDecisionTaskResponse> decisionTaskIterator) throws Exception {
        HistoryHelper historyHelper = new HistoryHelper(decisionTaskIterator);
        AsyncDecider decider = createDecider(historyHelper);
        decider.decide();
        return decider.getAsynchronousThreadDumpAsString();
    }

    private AsyncDecider createDecider(HistoryHelper historyHelper) throws Exception {
        PollForDecisionTaskResponse decisionTask = historyHelper.getDecisionTask();
        WorkflowType workflowType = fromSdkType(decisionTask.workflowType());
        if (log.isDebugEnabled()) {
            log.debug("WorkflowTask received: taskId=" + decisionTask.startedEventId() + ", taskToken="
                    + decisionTask.taskToken() + ", workflowExecution=" + decisionTask.workflowExecution());
        }
        WorkflowDefinitionFactory workflowDefinitionFactory = definitionFactoryFactory.getWorkflowDefinitionFactory(workflowType);
        if (workflowDefinitionFactory == null) {
            ThreadLocalMetrics.getMetrics().recordCount(MetricName.TYPE_NOT_FOUND.getName(), 1, MetricName.getWorkflowTypeDimension(workflowType));
            log.error("Received decision task for workflow type not configured with a worker: workflowType="
                    + decisionTask.workflowType() + ", taskToken=" + decisionTask.taskToken() + ", workflowExecution="
                    + decisionTask.workflowExecution());
            Iterable<WorkflowType> typesToRegister = definitionFactoryFactory.getWorkflowTypesToRegister();
            StringBuilder types = new StringBuilder();
            types.append("[");
            for (WorkflowType t : typesToRegister) {
                if (types.length() > 1) {
                    types.append(", ");
                }
                types.append(t);
            }
            types.append("]");
            throw new IncompatibleWorkflowDefinition("Workflow type \"" + workflowType
                    + "\" is not supported by the WorkflowWorker. "
                    + "Possible cause is workflow type version change without changing task list name. "
                    + "Workflow types registered by the worker are: " + types.toString());
        }
        // Check if we need to look for a cached decider
        if (affinityHelper != null && affinityHelper.isAffinityWorker()) {
            AsyncDecider decider = affinityHelper.getDeciderForDecisionTask(decisionTask);
            if (decider != null) {
                // Save the component versions and workflow context data of the history helper in the cached decider
                historyHelper.setComponentVersions(decider.getHistoryHelper().getComponentVersions());
                historyHelper.setWorkflowContextData(decider.getHistoryHelper().getWorkflowContextData());
                // Skip the DecisionTaskStarted event this cached decider has seen
                historyHelper.getSingleDecisionEvents();
                // Update the cached decider with new history events
                decider.setHistoryHelper(historyHelper);
                ThreadLocalMetrics.getMetrics().recordCount(MetricName.VALID_DECIDER_FOUND_IN_CACHE.getName(), 1,
                    MetricName.getWorkflowTypeDimension(fromSdkType(decisionTask.workflowType())));
                return decider;
            }
            // No valid decider is found in cache, so we need to replay using the entire history
            historyHelper = affinityHelper.createHistoryHelperForDecisionTask(decisionTask);
            ThreadLocalMetrics.getMetrics().recordCount(MetricName.VALID_DECIDER_FOUND_IN_CACHE.getName(), 0,
                MetricName.getWorkflowTypeDimension(fromSdkType(decisionTask.workflowType())));
        }
        DecisionsHelper decisionsHelper = new DecisionsHelper(decisionTask, childWorkflowIdHandler, clientConfig);
        WorkflowTypeImplementationOptions workflowImplementationOptions = workflowDefinitionFactory.getWorkflowImplementationOptions();
        if (workflowImplementationOptions != null) {
            List<WorkflowTypeComponentImplementationVersion> implementationComponentVersions = workflowImplementationOptions.getImplementationComponentVersions();
            historyHelper.getComponentVersions().setWorkflowImplementationComponentVersions(implementationComponentVersions);
        }
        return new AsyncDecider(workflowDefinitionFactory, historyHelper, decisionsHelper);
    }
}
