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
package com.amazonaws.services.simpleworkflow.flow.config;

import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecutionMetadata;
import com.amazonaws.services.simpleworkflow.flow.worker.AsyncDecider;
import java.time.Duration;
import java.util.function.Predicate;
import javax.cache.Cache;
import lombok.Getter;

public class DeciderAffinityConfig {
    @Getter
    private final Duration affinityTaskListScheduleToStartTimeout;

    @Getter
    private final Cache<WorkflowExecution, AsyncDecider> deciderCache;

    @Getter
    private final Predicate<WorkflowExecutionMetadata> establishAffinityPredicate;

    public DeciderAffinityConfig(Duration affinityTaskListScheduleToStartTimeout, Cache<WorkflowExecution, AsyncDecider> deciderCache) {
        this(affinityTaskListScheduleToStartTimeout, deciderCache, (workflowExecutionMetadata) -> true);
    }

    public DeciderAffinityConfig(Duration affinityTaskListScheduleToStartTimeout, Cache<WorkflowExecution, AsyncDecider> deciderCache, Predicate<WorkflowExecutionMetadata> establishAffinityPredicate) {
        if (affinityTaskListScheduleToStartTimeout == null) {
            throw new IllegalArgumentException("Trying to create a decider affinity config while affinityTaskListScheduleToStartTimeout is not provided");
        }
        if (deciderCache == null) {
            throw new IllegalArgumentException("Trying to create a decider affinity config while deciderCache is not provided");
        }
        if (establishAffinityPredicate == null) {
            throw new IllegalArgumentException("Trying to create a decider affinity config while setting establishAffinityPredicate to null");
        }
        this.affinityTaskListScheduleToStartTimeout = affinityTaskListScheduleToStartTimeout;
        this.deciderCache = deciderCache;
        this.establishAffinityPredicate = establishAffinityPredicate;
    }

}
