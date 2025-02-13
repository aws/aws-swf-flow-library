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

@SuppressWarnings("serial")
public class LambdaFunctionException extends DecisionException {

    private String functionName;

    private String functionId;

    public LambdaFunctionException(String message) {
        super(message);
    }

    public LambdaFunctionException(String message, Throwable cause) {
        super(message, cause);
    }

    public LambdaFunctionException(String message, long eventId, String name, String id) {
        super(message + " for functionId=\"" + id + "\" of functionName=" + name, eventId);
        this.functionName = name;
        this.functionId = id;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getFunctionId() {
        return functionId;
    }

    public void setFunctionId(String functionId) {
        this.functionId = functionId;
    }
}
