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

import software.amazon.awssdk.services.swf.model.StartLambdaFunctionFailedCause;

public class StartLambdaFunctionFailedException extends LambdaFunctionException {

    private StartLambdaFunctionFailedCause failureCause;

    public StartLambdaFunctionFailedException(String message) {
        super(message);
    }

    public StartLambdaFunctionFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public StartLambdaFunctionFailedException(long eventId, String functionName, String functionId, String cause) {
        super(cause, eventId, functionName, functionId);
        failureCause = StartLambdaFunctionFailedCause.fromValue(cause);
    }

    /**
     * @return enumeration that contains the cause of the failure
     */
    public StartLambdaFunctionFailedCause getFailureCause() {
        return failureCause;
    }

    public void setFailureCause(StartLambdaFunctionFailedCause failureCause) {
        this.failureCause = failureCause;
    }
}
