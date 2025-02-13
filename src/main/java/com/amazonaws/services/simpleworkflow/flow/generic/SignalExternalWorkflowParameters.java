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
package com.amazonaws.services.simpleworkflow.flow.generic;

public class SignalExternalWorkflowParameters implements Cloneable {

    private String input;
    
    private String runId;
    
    private String signalName;
    
    private String workflowId;
    
    public SignalExternalWorkflowParameters() {
    }
    
    public String getInput() {
        return input;
    }
    
    public void setInput(String input) {
        this.input = input;
    }
    
    public SignalExternalWorkflowParameters withInput(String input) {
        this.input = input;
        return this;
    }
    
    public String getRunId() {
        return runId;
    }
    
    public void setRunId(String runId) {
        this.runId = runId;
    }
    
    public SignalExternalWorkflowParameters withRunId(String runId) {
        this.runId = runId;
        return this;
    }

    public String getSignalName() {
        return signalName;
    }
    
    public void setSignalName(String signalName) {
        this.signalName = signalName;
    }
    
    public SignalExternalWorkflowParameters withSignalName(String signalName) {
        this.signalName = signalName;
        return this;
    }
    
    public String getWorkflowId() {
        return workflowId;
    }
    
    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }
    
    public SignalExternalWorkflowParameters withWorkflowId(String workflowId) {
        this.workflowId = workflowId;
        return this;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("SignalName: " + signalName + ", ");
        sb.append("Input: " + input + ", ");
        sb.append("WorkflowId: " + workflowId + ", ");
        sb.append("RunId: " + runId + ", ");
        sb.append("}");
        return sb.toString();
    }

    public SignalExternalWorkflowParameters clone() {
        SignalExternalWorkflowParameters result = new SignalExternalWorkflowParameters();
        result.setInput(input);
        result.setRunId(runId);
        result.setSignalName(signalName);
        result.setWorkflowId(workflowId);
        return result;
    }
    
    
}
