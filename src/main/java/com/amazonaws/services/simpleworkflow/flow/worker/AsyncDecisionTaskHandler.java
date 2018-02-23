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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.simpleworkflow.flow.core.AsyncTaskInfo;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinitionFactoryFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeComponentImplementationVersion;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowTypeImplementationOptions;
import com.amazonaws.services.simpleworkflow.model.Decision;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.RespondDecisionTaskCompletedRequest;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;

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

    private final boolean skipFailedCheck;
    
    public AsyncDecisionTaskHandler(WorkflowDefinitionFactoryFactory definitionFactoryFactory) {
        this(definitionFactoryFactory, false);
    }
    
    public AsyncDecisionTaskHandler(WorkflowDefinitionFactoryFactory definitionFactoryFactory, boolean skipFailedCheck) {
        this.definitionFactoryFactory = definitionFactoryFactory;
        this.skipFailedCheck = skipFailedCheck;
    }

    @Override
    public RespondDecisionTaskCompletedRequest handleDecisionTask(Iterator<DecisionTask> decisionTaskIterator) throws Exception {
        HistoryHelper historyHelper = new HistoryHelper(decisionTaskIterator);
        AsyncDecider decider = createDecider(historyHelper);
        decider.decide();
        DecisionsHelper decisionsHelper = decider.getDecisionsHelper();
        Collection<Decision> decisions = decisionsHelper.getDecisions();
        DecisionTask decisionTask = historyHelper.getDecisionTask();
        if (log.isDebugEnabled()) {
            log.debug("WorkflowTask taskId=" + decisionTask.getStartedEventId() + ", taskToken=" + decisionTask.getTaskToken()
                    + " completed with " + decisions.size() + " new decisions");
        }
        if (decisions.size() == 0 && asyncThreadDumpLog.isTraceEnabled()) {
            asyncThreadDumpLog.trace("Empty decision list with the following waiting tasks:\n"
                    + decider.getAsynchronousThreadDumpAsString());
        }
        RespondDecisionTaskCompletedRequest completedRequest = new RespondDecisionTaskCompletedRequest();
        completedRequest.setTaskToken(decisionTask.getTaskToken());
        completedRequest.setDecisions(decisions);
        String contextData = decisionsHelper.getWorkflowContextDataToReturn();
        ComponentVersions componentVersions = historyHelper.getComponentVersions();
        Map<String, Integer> versionsToSave = componentVersions.getVersionsToSave();
        String executionContext = getExecutionContext(contextData, versionsToSave);
        if (historyHelper.getWorkflowContextData() == null || !historyHelper.getWorkflowContextData().equals(executionContext)) {
            completedRequest.setExecutionContext(executionContext);
        }
        return completedRequest;
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
    public WorkflowDefinition loadWorkflowThroughReplay(Iterator<DecisionTask> decisionTaskIterator) throws Exception {
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
    public List<AsyncTaskInfo> getAsynchronousThreadDump(Iterator<DecisionTask> decisionTaskIterator) throws Exception {
        HistoryHelper historyHelper = new HistoryHelper(decisionTaskIterator);
        AsyncDecider decider = createDecider(historyHelper);
        decider.decide();
        return decider.getAsynchronousThreadDump();
    }

    @Override
    public String getAsynchronousThreadDumpAsString(Iterator<DecisionTask> decisionTaskIterator) throws Exception {
        HistoryHelper historyHelper = new HistoryHelper(decisionTaskIterator);
        AsyncDecider decider = createDecider(historyHelper);
        decider.decide();
        return decider.getAsynchronousThreadDumpAsString();
    }

    private AsyncDecider createDecider(HistoryHelper historyHelper) throws Exception {
        DecisionTask decisionTask = historyHelper.getDecisionTask();
        WorkflowType workflowType = decisionTask.getWorkflowType();
        if (log.isDebugEnabled()) {
            log.debug("WorkflowTask received: taskId=" + decisionTask.getStartedEventId() + ", taskToken="
                    + decisionTask.getTaskToken() + ", workflowExecution=" + decisionTask.getWorkflowExecution());
        }
        WorkflowDefinitionFactory workflowDefinitionFactory = definitionFactoryFactory.getWorkflowDefinitionFactory(workflowType);
        if (workflowDefinitionFactory == null) {
            log.error("Received decision task for workflow type not configured with a worker: workflowType="
                    + decisionTask.getWorkflowType() + ", taskToken=" + decisionTask.getTaskToken() + ", workflowExecution="
                    + decisionTask.getWorkflowExecution());
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
        DecisionsHelper decisionsHelper = new DecisionsHelper(decisionTask);
        WorkflowTypeImplementationOptions workflowImplementationOptions = workflowDefinitionFactory.getWorkflowImplementationOptions();
        if (workflowImplementationOptions != null) {
            List<WorkflowTypeComponentImplementationVersion> implementationComponentVersions = workflowImplementationOptions.getImplementationComponentVersions();
            historyHelper.getComponentVersions().setWorkflowImplementationComponentVersions(implementationComponentVersions);
        }
        AsyncDecider decider = new AsyncDecider(workflowDefinitionFactory, historyHelper, decisionsHelper);
        return decider;
    }
}
