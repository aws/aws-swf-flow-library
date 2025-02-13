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
package com.amazonaws.services.simpleworkflow.flow.common;

/**
 * This enum is for internal use only and may be changed or removed without prior notice.
 *
 */
public enum FlowValueConstraint {
    
    FAILURE_REASON("FAILURE_REASON", 0, 256),
    
    FAILURE_DETAILS("FAILURE_DETAILS", 0, 32768);
    
    private final String value;

    private final int min;

    private final int max;

    private FlowValueConstraint(String value, int min, int max) {
        this.value = value;
        this.min = min;
        this.max = max;
    }
    
    @Override
    public String toString() {
        return value;
    }

    public int getMinSize() {
        return min;
    }

    public int getMaxSize() {
        return max;
    }
    
    public static FlowValueConstraint fromValue(String value) {
        if (value == null || "".equals(value)) {
            throw new IllegalArgumentException("Value cannot be null or empty!");
        }
        else if ("FAILURE_REASON".equals(value)) {
            return FlowValueConstraint.FAILURE_REASON;
        }
        else if ("FAILURE_DETAILS".equals(value)) {
            return FlowValueConstraint.FAILURE_DETAILS;
        }
        else {
            throw new IllegalArgumentException("Cannot create enum from " + value + " value!");
        }
    }

}
