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
package com.amazonaws.services.simpleworkflow.flow.interceptors;

import java.util.concurrent.Executor;

/**
 * Serves the same purpose as {@link Executor}, but in asynchronous world. The
 * difference from {@link Executor} is that checked exceptions are not treated
 * differently then runtime ones as {@link AsyncRunnable#run()} throws
 * Throwable.
 */
public interface AsyncExecutor {

    public void execute(AsyncRunnable command);

}
