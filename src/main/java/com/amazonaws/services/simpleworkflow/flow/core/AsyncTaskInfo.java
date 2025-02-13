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
package com.amazonaws.services.simpleworkflow.flow.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public final class AsyncTaskInfo {

    private final StackTraceElement[] asyncStackTrace;

    private final boolean daemon;

    private final Promise<?>[] waitingFor;

    private final String name;

    public AsyncTaskInfo(String name, StackTraceElement[] asyncStackTrace, boolean daemon, Promise<?>[] waitFor) {
        this.name = name;
        this.asyncStackTrace = asyncStackTrace;
        this.daemon = daemon;
        this.waitingFor = waitFor;
    }

    public String getName() {
        return name;
    }

    public StackTraceElement[] getAsyncStackTrace() {
        return asyncStackTrace;
    }

    public boolean isDaemon() {
        return daemon;
    }

    public Promise<?>[] getWaitingFor() {
        return waitingFor;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (name != null) {
            result.append("\"");
            result.append(name);
            result.append("\"");
        }
        if (daemon) {
            if (result.length() > 0) {
                result.append(" ");
            }
            result.append("daemon");
        }
        if (waitingFor != null) {
            Map<Integer, String> waitingOnArguments = new HashMap<Integer, String>();
            for (int i = 0; i < waitingFor.length; i++) {
                Promise<?> promise = waitingFor[i];
                if (promise != null && !promise.isReady()) {
                    if (promise instanceof AndPromise) {
                        AndPromise andPromise = (AndPromise) promise;
                        Promise<?>[] elements = andPromise.getValues();
                        StringBuilder description = new StringBuilder();
                        description.append("PromiseCollection[");
                        boolean first = true;
                        for (int j = 0; j < elements.length; j++) {
                            Promise<?> e = elements[j];
                            if (e == null) {
                                continue;
                            }
                            if (first) {
                                first = false;
                            } else {
                                description.append(" ");
                            }
                            description.append(j);
                            String d = e.getDescription();
                            if (d != null) {
                                description.append(":\"");
                                description.append(d);
                                description.append("\"");
                            }
                        }
                        description.append("]");
                        waitingOnArguments.put(i + 1, description.toString());
                    }
                    else {
                        String quotedDescription = promise.getDescription() == null ? null : "\"" + promise.getDescription() + "\"";
                        waitingOnArguments.put(i + 1,  quotedDescription);
                    }
                }
            }
            if (waitingOnArguments.size() > 0) {
                if (result.length() > 0) {
                    result.append(" ");
                }
                result.append("waiting on argument");
                if (waitingOnArguments.size() > 1) {
                    result.append("s");
                }
                result.append(" (starting from 1)");
                for (Entry<Integer, String> pair : waitingOnArguments.entrySet()) {
                    result.append(" ");
                    result.append(pair.getKey());
                    String description = pair.getValue();
                    if (description != null) {
                        result.append(":");
                        result.append(description);
                    }
                }
            }
        }
        if (result.length() > 0) {
            result.append("\n");
        }
        if (asyncStackTrace != null) {
            for (int i = 0; i < asyncStackTrace.length; i++) {
                result.append("\tat ");
                result.append(asyncStackTrace[i]);
                result.append("\n");
            }
        }
        else {
            result.append("Async Trace is Disabled.");
        }
        return result.toString();
    }
}
