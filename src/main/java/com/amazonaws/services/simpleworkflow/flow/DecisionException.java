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

/**
 * Exception used to communicate failure during fulfillment of a decision sent
 * to SWF. This exception and all its subclasses are expected to be thrown by
 * the framework. The only reason its constructor is public is so allow unit
 * tests that throw it.
 */
@SuppressWarnings("serial")
public abstract class DecisionException extends RuntimeException {

    private long eventId;
    
    public DecisionException(String message) {
        super(message);
    }

    public DecisionException(String message, Throwable cause) {
        super(message, cause);
    }

    public DecisionException(String message, long eventId) {
        super(message);
        this.eventId = eventId;
    }

    public long getEventId() {
        return eventId;
    }
    
    public void setEventId(long eventId) {
        this.eventId = eventId;
    }
}
