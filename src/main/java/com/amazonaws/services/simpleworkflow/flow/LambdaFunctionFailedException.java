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
public class LambdaFunctionFailedException extends LambdaFunctionException {

    private String detail;

    public LambdaFunctionFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public LambdaFunctionFailedException(String message) {
        super(message);
    }

    public LambdaFunctionFailedException(long eventId, String name, String id, String detail) {
        super("failed", eventId, name, id);
        this.detail = detail;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

}
