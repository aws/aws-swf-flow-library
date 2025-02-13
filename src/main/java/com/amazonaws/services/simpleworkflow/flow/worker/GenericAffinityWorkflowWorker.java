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

package com.amazonaws.services.simpleworkflow.flow.worker;

import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import software.amazon.awssdk.services.swf.SwfClient;

public class GenericAffinityWorkflowWorker extends GenericWorkflowWorker {

    public static final String AFFINITY_THREAD_NAME_PREFIX = "SWF Affinity Decider ";

    private final AffinityHelper affinityHelper;

    public GenericAffinityWorkflowWorker(SwfClient service, String domain, SimpleWorkflowClientConfig config) {
        super();
        if (config.getDeciderAffinityConfig() == null) {
            throw new IllegalStateException("Trying to create an affinity worker while decider affinity config is not provided");
        }
        setService(service);
        setDomain(domain);
        affinityHelper = new AffinityHelper(getService(), getDomain(), config, true);
        setTaskListToPoll(affinityHelper.getAffinityTaskList());
        setClientConfig(config);
    }

    @Override
    protected TaskPoller<DecisionTaskPoller.DecisionTaskIterator> createPoller() {
        DecisionTaskPoller result = (DecisionTaskPoller) super.createPoller();
        AsyncDecisionTaskHandler decisionTaskHandler = new AsyncDecisionTaskHandler(workflowDefinitionFactoryFactory, childWorkflowIdHandler, affinityHelper, getClientConfig());
        result.setDecisionTaskHandler(decisionTaskHandler);
        return result;
    }

    @Override
    protected String getPollThreadNamePrefix() {
        return AFFINITY_THREAD_NAME_PREFIX + getTaskListToPoll();
    }
}
