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

import java.util.concurrent.Executor;

interface AsyncParentContext {

    void add(AsyncContextBase async, Promise<?> waitFor);

    void remove(AsyncContextBase async);

    void fail(AsyncContextBase async, Throwable e);

    /**
     * Only Task passes daemon flag to its heirs. Daemon TryCatchFinally doesn't
     * pass it to heirs as cancellation of the daemon TryCatchFinally causes
     * heirs cancellation independently of their daemon status.
     */
    boolean getDaemonFlagForHeir();

    Executor getExecutor();

    AsyncStackTrace getStackTrace();
    
    String getParentTaskMethodName();

    boolean isRethrown(Throwable e);

    AsyncParentContext getCurrentTryCatchFinallyContext();

    boolean getHideStartFromMethod();

    String getName();

    boolean isCancelRequested();

}
