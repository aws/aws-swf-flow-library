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
package com.amazonaws.services.simpleworkflow.flow.test;

import com.amazonaws.services.simpleworkflow.flow.ActivityExecutionContext;
import com.amazonaws.services.simpleworkflow.flow.ActivityFailureException;
import com.amazonaws.services.simpleworkflow.flow.ActivityTaskFailedException;
import com.amazonaws.services.simpleworkflow.flow.ActivityTaskTimedOutException;
import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.DecisionContextProvider;
import com.amazonaws.services.simpleworkflow.flow.DecisionContextProviderImpl;
import com.amazonaws.services.simpleworkflow.flow.ScheduleActivityTaskFailedException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowConstants;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementation;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementationFactory;
import com.amazonaws.services.simpleworkflow.flow.generic.ExecuteActivityParameters;
import com.amazonaws.services.simpleworkflow.flow.generic.GenericActivityClient;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityType;
import com.amazonaws.services.simpleworkflow.flow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeRegistrationOptions;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.ActivityTaskTimeoutType;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskFailedCause;

public class TestGenericActivityClient implements GenericActivityClient {

    private final class TestActivityExecutionContext extends ActivityExecutionContext {

        private final WorkflowExecution workflowExecution;

        private TestActivityExecutionContext(ActivityTask activityTask, WorkflowExecution workflowExecution) {
            super(activityTask);
            this.workflowExecution = workflowExecution;
        }

        @Override
        public void recordActivityHeartbeat(String details) throws AwsServiceException, SdkException {
            //TODO: timeouts
        }

        @Override
        public SwfClient getService() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public WorkflowExecution getWorkflowExecution() {
            return workflowExecution;
        }

        @Override
        public String getDomain() {
            return "dummyTestDomain";
        }
        
    }

    /**
     * Key is TaskList
     */
    protected final Map<String, ActivityImplementationFactory> factories = new HashMap<String, ActivityImplementationFactory>();

    protected final Map<ActivityType, ActivityTypeRegistrationOptions> registrationOptions = new HashMap<ActivityType, ActivityTypeRegistrationOptions>();

    protected final Map<ActivityType, String> workerTaskLists = new HashMap<ActivityType, String>();

    protected final DecisionContextProvider decisionContextProvider;

    public TestGenericActivityClient(DecisionContextProvider decisionContextProvider) {
        this.decisionContextProvider = decisionContextProvider;
    }

    public TestGenericActivityClient() {
        this(new DecisionContextProviderImpl());
    }

    public void addFactory(String taskListToListen, ActivityImplementationFactory factory) {
        factories.put(taskListToListen, factory);
        Iterable<ActivityType> typesToRegister = factory.getActivityTypesToRegister();
        for (ActivityType activityType : typesToRegister) {
            ActivityImplementation implementation = factory.getActivityImplementation(activityType);
            ActivityTypeRegistrationOptions ro = implementation.getRegistrationOptions();
            registrationOptions.put(activityType, ro);
            workerTaskLists.put(activityType, taskListToListen);
        }
    }

    @Override
    public Promise<String> scheduleActivityTask(final ExecuteActivityParameters parameters) {
        final ActivityType activityType = parameters.getActivityType();
        final Settable<String> result = new Settable<String>();
        String activityId = parameters.getActivityId();
        if (activityId == null) {
            activityId = decisionContextProvider.getDecisionContext().getWorkflowClient().generateUniqueId();
        }

        DecisionContext decisionContext = decisionContextProvider.getDecisionContext();
        final WorkflowExecution workflowExecution = decisionContext.getWorkflowContext().getWorkflowExecution();

        ActivityTask activityTask = ActivityTask.builder()
            .activityId(activityId)
            .activityType(activityType)
            .input(parameters.getInput())
            .startedEventId(0L)
            .taskToken("dummyTaskToken")
            .workflowExecution(workflowExecution).build();

        String taskList = parameters.getTaskList();
        if (taskList == null) {
            ActivityTypeRegistrationOptions ro = registrationOptions.get(activityType);
            if (ro == null) {
                String cause = ScheduleActivityTaskFailedCause.ACTIVITY_TYPE_DOES_NOT_EXIST.toString();
                throw new ScheduleActivityTaskFailedException(0, activityType, activityId, cause);
            }
            taskList = ro.getDefaultTaskList();
            if (FlowConstants.NO_DEFAULT_TASK_LIST.equals(taskList)) {
                String cause = ScheduleActivityTaskFailedCause.DEFAULT_TASK_LIST_UNDEFINED.toString();
                throw new ScheduleActivityTaskFailedException(0, activityType, activityId, cause);
            } else if (taskList == null) {
                taskList = workerTaskLists.get(activityType);
            }
        }
        ActivityImplementationFactory factory = factories.get(taskList);
        // Nobody listens on the specified task list. So in case of a real service it causes 
        // ScheduleToStart timeout.
        //TODO: Activity heartbeats and passing details to the exception.
        if (factory == null) {
            String timeoutType = ActivityTaskTimeoutType.SCHEDULE_TO_START.toString();
            throw new ActivityTaskTimedOutException(0, activityType, activityId, timeoutType, null);
        }
        final ActivityImplementation impl = factory.getActivityImplementation(activityType);
        if (impl == null) {
            String cause = ScheduleActivityTaskFailedCause.ACTIVITY_TYPE_DOES_NOT_EXIST.toString();
            throw new ScheduleActivityTaskFailedException(0, activityType, activityId, cause);
        }
        ActivityExecutionContext executionContext = new TestActivityExecutionContext(activityTask, workflowExecution);
        try {
            String activityResult = impl.execute(executionContext);
            result.set(activityResult);
        } catch (Throwable e) {
            if (e instanceof ActivityFailureException) {
                ActivityFailureException falure = (ActivityFailureException) e;
                throw new ActivityTaskFailedException(0, activityType, parameters.getActivityId(), falure.getReason(),
                        falure.getDetails());
            }
            // Unless there is problem in the framework or generic activity implementation this shouldn't be executed
            ActivityTaskFailedException failure = new ActivityTaskFailedException(0, activityType, parameters.getActivityId(),
                    e.getMessage(), null);
            failure.initCause(e);
            throw failure;
        }
        return result;
    }

    @Override
    public Promise<String> scheduleActivityTask(String activity, String version, String input) {
        ExecuteActivityParameters parameters = new ExecuteActivityParameters();
        ActivityType activityType = ActivityType.builder().name(activity).version(version).build();
        parameters.setActivityType(activityType);
        parameters.setInput(input);
        return scheduleActivityTask(parameters);
    }

    @Override
    public Promise<String> scheduleActivityTask(final String activity, final String version, final Promise<String> input) {
        final Settable<String> result = new Settable<String>();
        new Task(input) {

            @Override
            protected void doExecute() throws Throwable {
                result.chain(scheduleActivityTask(activity, version, input.get()));
            }
        };
        return result;
    }

}
