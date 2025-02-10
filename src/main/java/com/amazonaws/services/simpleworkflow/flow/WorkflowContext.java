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

import java.util.List;

import com.amazonaws.services.simpleworkflow.flow.annotations.WorkflowComponentImplementationVersions;
import com.amazonaws.services.simpleworkflow.flow.generic.ContinueAsNewWorkflowExecutionParameters;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowType;
import software.amazon.awssdk.services.swf.model.ChildPolicy;


public interface WorkflowContext {

    WorkflowExecution getWorkflowExecution();
    
    WorkflowExecution getParentWorkflowExecution();
    
    WorkflowType getWorkflowType();
    
    boolean isCancelRequested();
    
    ContinueAsNewWorkflowExecutionParameters getContinueAsNewOnCompletion();
    
    void setContinueAsNewOnCompletion(ContinueAsNewWorkflowExecutionParameters continueParameters);

    List<String> getTagList();

    ChildPolicy getChildPolicy();

    String getContinuedExecutionRunId();

    long getExecutionStartToCloseTimeout();

    String getTaskList();
    
    int getTaskPriority();

    String getLambdaRole();
    
    /**
     * Is the component current version greater or equal to the passed
     * parameter? Increases the current version if it is newly executed workflow
     * code (not replay) and the version passed as a parameter is bigger then
     * the current version.
     * <p>
     * This method is created to enable workflow updates without changing their
     * type version. The code changes should follow the following pattern:
     *
     * <pre>
     * if (workflowContext.isImplementationVersion(COMPONENT_NAME, 5)) {
     *     // New code path
     * }
     * else if (workflowContext.isImplementationVersion(COMPONENT_NAME, 4)) {
     *     // Old version 4 code path
     * }
     * else if (workflowContext.isImplementationVersion(COMPONENT_NAME, 3)) {
     *     // Even older version 3 code path
     * }
     * else {
     *     // Code path for version 2 and below was already removed
     *     throw new IncompatibleWorkflowDefinition(&quot;Implementation version below 3 is not supported for &quot; + COMPONENT_NAME);
     * }
     * </pre>
     * <p>
     * The "old code path" branches are used to reconstruct through replay the
     * state of workflows that already executed it. The "new code path" branch
     * is executed for workflows that haven't reached it before the update. As
     * soon as all workflows for the old code path are closed the condition as
     * well as the old path can be removed. Conditions should start from higher
     * version and finish with the lowest supported one as
     * <code>isVersion</code> returns true for any version that is lower then
     * the current one.
     * <p>
     * <code>componentName</code> parameter links multiple such conditions
     * together. All conditions that share the same component value return the
     * same result for the same version. In the following example as the first
     * call to isVersion happens in the part of the workflow that has already
     * executed the second call to isVersion returns <code>false</code> even if
     * it is executed for the first time.
     *
     * <pre>
     * if (workflowContext.isImplementationVersion(&quot;comp1&quot;, 1)) {
     *     // New code path 1
     * }
     * else {
     *     // Old code path 1
     * }
     * 
     * // Location of the workflow execution when upgrade was deployed.
     * 
     * if (workflowContext.isImplementationVersion(&quot;comp1&quot;, 1)) {
     *     // New code path 2
     * }
     * else {
     *     // Old code path 2
     * }
     * </pre>
     * <p>
     * If all updates are independent then they should use different component
     * names which allows them to take a new code path independently of other
     * components.
     * <p>
     * Use {@link WorkflowComponentImplementationVersions} annotation to specify
     * maximum and minimum supported as well as allowed version for each
     * component.
     * <p>
     * Maximum and minimum supported versions define valid range of version
     * values that given component used in a workflow implementation supports.
     * Call to <code>isImplementationVersion</code> with value out of this range
     * causes decision failure. Failed decisions are retried after a configured
     * decision task timeout. So "bad deployment" that starts workers that are
     * not compatible with currently open workflow histories results in a few
     * decision falures, but doesn't affect open workflows correctness. Another
     * situation is rolling deployment when both new and old workers are active.
     * During such deployment histories processed by the new version fail to be
     * processed by the old workers. If such deployment is not very long running
     * it might be acceptable to fail and retry (after a timeout) a few
     * decisions.
     * <p>
     * Maximum allowed version enables version upgrades without any decision
     * failures. When it is set to a lower value then maximum supported version
     * the worklfow implementation takes code path that corresponds to it for
     * the newly executed code. When performing replay the newer code path (up
     * to the maximum supported version) can be taken. The decision failure free
     * upgrades are done by the following steps:
     * <ol>
     * <li>It is expected that the currently running workers have the same
     * allowed and supported versions.</li>
     * <li>New workers are deployed with maximum allowed version set to the same
     * version as the currently running workers. Even if rolling deployment is
     * used no decisions should fail.</li>
     * <li>Second deployment of the new workers is done with maximum allowed
     * version set to the same as their maximum supported version. To avoid code
     * change for the second deployment instead of
     * {@link WorkflowComponentImplementationVersions} annotation use
     * {@link WorkflowWorker#setMaximumAllowedComponentImplementationVersions(java.util.Map)}
     * to specify the maximum allowed version.</li>
     * </ol>
     * 
     * 
     * @param componentName
     *            name of the versioned component
     * @param internalVersion
     *            internal component version
     * @return if the code path of the specified version should be taken.
     */
    boolean isImplementationVersion(String componentName, int internalVersion);
 
    /**
     * The current version of the component.
     * 
     * @param component
     *            name of the component to version
     * @return <code>null</code> if no version found for the component.
     */
    Integer getVersion(String component);

}
