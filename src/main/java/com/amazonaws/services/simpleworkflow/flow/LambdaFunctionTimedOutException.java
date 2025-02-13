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

import software.amazon.awssdk.services.swf.model.LambdaFunctionTimeoutType;

@SuppressWarnings("serial")
public class LambdaFunctionTimedOutException extends LambdaFunctionException {

    private LambdaFunctionTimeoutType timeoutType;

    public LambdaFunctionTimedOutException(String message, Throwable cause) {
        super(message, cause);
    }

    public LambdaFunctionTimedOutException(String message) {
        super(message);
    }

    public LambdaFunctionTimedOutException(long eventId, String lambdaFunctionName, String lambdaId, String timeoutType) {
        super(timeoutType, eventId, lambdaFunctionName, lambdaId);
        this.timeoutType = LambdaFunctionTimeoutType.fromValue(timeoutType);
    }

    public LambdaFunctionTimeoutType getTimeoutType() {
        return timeoutType;
    }

    public void setTimeoutType(LambdaFunctionTimeoutType timeoutType) {
        this.timeoutType = timeoutType;
    }
}
