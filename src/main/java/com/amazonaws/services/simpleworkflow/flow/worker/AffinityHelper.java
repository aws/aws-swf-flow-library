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

import static com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution.fromSdkType;

import com.amazonaws.services.simpleworkflow.flow.config.SimpleWorkflowClientConfig;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;
import lombok.Getter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;

public class AffinityHelper {

    private static final Log log = LogFactory.getLog(AffinityHelper.class);

    private SwfClient service;

    private String domain;

    private SimpleWorkflowClientConfig config;

    @Getter
    private boolean affinityWorker;

    @Getter
    private String affinityTaskList;

    private static final long MAXIMUM_PAGE_SIZE = 1000;

    private static final double FORCE_FETCH_FULL_HISTORY_RATIO = 0.3;

    public AffinityHelper(SwfClient service, String domain, SimpleWorkflowClientConfig config, boolean affinityWorker) {
        this.service = service;
        this.domain = domain;
        this.config = config;
        this.affinityWorker = affinityWorker;
        this.affinityTaskList = getHostName();
    }

    public HistoryHelper createHistoryHelperForDecisionTask(PollForDecisionTaskResponse decisionTask) {
       return new HistoryHelper(new WorkflowHistoryDecisionTaskIterator(service, domain, decisionTask, config));
    }

    public AsyncDecider getDeciderForDecisionTask(PollForDecisionTaskResponse decisionTask) {
        WorkflowExecution workflowExecution = fromSdkType(decisionTask.workflowExecution());
        AsyncDecider decider = config.getDeciderAffinityConfig().getDeciderCache().get(workflowExecution);
        if (isValidDecider(decisionTask, decider)) {
            return decider;
        } else {
            config.getDeciderAffinityConfig().getDeciderCache().remove(workflowExecution);
            return null;
        }
    }

    public WorkflowHistoryDecisionTaskIterator getHistoryIterator(PollForDecisionTaskResponse decisionTask) {
        return new WorkflowHistoryDecisionTaskIterator(service, domain, decisionTask, config);
    }

    public boolean shouldForceFetchFullHistory(PollForDecisionTaskResponse decisionTask) {
        return decisionTask.startedEventId().longValue() > MAXIMUM_PAGE_SIZE && ThreadLocalRandom.current().nextDouble() < FORCE_FETCH_FULL_HISTORY_RATIO;
    }

    private String getHostName() {
        String hostname = System.getenv("HOSTNAME");
        String result;
        if (hostname != null && !"localhost".equalsIgnoreCase(hostname)) {
            result = hostname;
        } else {
            try {
                result = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                // Affinity worker will fail to start if we cannot get host name for constructing the affinity task list.
                // In this case, Flow will run as if decider affinity is disabled.
                if (config.getDeciderAffinityConfig() != null) {
                    ThreadLocalMetrics.getMetrics().recordCount(MetricName.AFFINITY_WORKER_START_FAILURE.getName(), 1);
                    log.error("Unable to get host name when constructing affinity task list. Flow will run as if decider affinity is disabled.");
                }
                return null;
            }
        }
        if (result.length() > 256) {
            return result.substring(0, 256);
        }
        return result;
    }

    private boolean isValidDecider(PollForDecisionTaskResponse decisionTask, AsyncDecider decider) {
        // Make sure the cached decider has handled the previous decision task
        return decider != null && decisionTask.previousStartedEventId().equals(decider.getHistoryHelper().getDecisionTask().startedEventId());
    }
}
