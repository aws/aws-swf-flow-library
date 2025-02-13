/**
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow;

import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;

/**
 * Clock that must be used inside workflow definition code to ensure replay
 * determinism.
 */
public interface WorkflowClock {

    /**
     * @return time of the {@link PollForDecisionTaskResponse} start event of the decision
     *         being processed or replayed.
     */
    public long currentTimeMillis();

    /**
     * <code>true</code> indicates if workflow is replaying already processed
     * events to reconstruct it state. <code>false</code> indicates that code is
     * making forward process for the first time. For example can be used to
     * avoid duplicating log records due to replay.
     *
     * @return boolean - true if workflow is in replay mode, false if executing for the first time
     */
    public boolean isReplaying();

    /**
     * Create a Value that becomes ready after the specified delay.
     * 
     * @param delaySeconds
     *            time-interval after which the Value becomes ready in seconds.
     * @return Promise that becomes ready after the specified delay.
     */
    public abstract Promise<Void> createTimer(long delaySeconds);

    /**
     * Create a Value that becomes ready after the specified delay.
     *
     * @param <T> - the type of the context object
     * @param delaySeconds - the number of seconds to wait before the timer fires
     * @param context
     *            context object that is returned inside the value when it
     *            becomes ready.
     * @return Promise that becomes ready after the specified delay. When ready
     *         it contains value passed as context parameter.
     */
    public abstract <T> Promise<T> createTimer(long delaySeconds, final T context);
    
    /**
     * Create a Value that becomes ready after the specified delay and the provided timerId.
     *
     * @param <T> - the type of the context object
     * @param delaySeconds - the number of seconds to wait before the timer fires
     * @param context - context object that is returned inside the Promise when it becomes ready
     * @param timerId The Id for the timer.
     * @return Promise that becomes ready after the specified delay. When ready
     *         it contains value passed as context parameter.
     */
    public abstract <T> Promise<T> createTimer(long delaySeconds, final T context, String timerId);

}
