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
package com.amazonaws.services.simpleworkflow.flow.pojo;

import com.amazonaws.services.simpleworkflow.flow.ActivityExecutionContext;
import com.amazonaws.services.simpleworkflow.flow.ActivityFailureException;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.DataConverterException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.FlowValueConstraint;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementationBase;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeExecutionOptions;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.worker.CurrentActivityExecutionContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class POJOActivityImplementation extends ActivityImplementationBase {

    private static final Log log = LogFactory.getLog(POJOActivityImplementation.class);

    private final Method activity;

    private final Object activitiesImplementationObject;

    private final ActivityTypeExecutionOptions executionOptions;

    private final DataConverter converter;

    private final ActivityTypeRegistrationOptions registrationOptions;

    public POJOActivityImplementation(Object activitiesImplementationObject, Method activity,
                                      ActivityTypeRegistrationOptions registrationOptions, ActivityTypeExecutionOptions executionOptions,
                                      DataConverter converter) {
        this.activitiesImplementationObject = activitiesImplementationObject;
        this.activity = activity;
        this.registrationOptions = registrationOptions;
        this.executionOptions = executionOptions;
        this.converter = converter;
    }

    @Override
    protected String execute(String input, ActivityExecutionContext context) throws ActivityFailureException, CancellationException {
        Object[] inputParameters = ThreadLocalMetrics.getMetrics().recordSupplier(
            () -> converter.fromData(input, Object[].class),
            converter.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_DESERIALIZE.getName(),
            TimeUnit.MILLISECONDS
        );
        CurrentActivityExecutionContext.set(context);
        Object result = null;
        try {
            // Fill missing parameters with default values to make addition of new parameters backward compatible
            inputParameters = FlowHelpers.getInputParameters(activity.getParameterTypes(), inputParameters);
            result = activity.invoke(activitiesImplementationObject, inputParameters);
        }
        catch (InvocationTargetException invocationException) {
            throwActivityFailureException(invocationException.getTargetException() != null ? invocationException.getTargetException()
                    : invocationException);
        }
        catch (IllegalArgumentException | IllegalAccessException illegalArgumentException) {
            throwActivityFailureException(illegalArgumentException);
        } finally {
            CurrentActivityExecutionContext.unset();
        }
        final Object finalResult = result;
        return ThreadLocalMetrics.getMetrics().recordSupplier(
            () -> converter.toData(finalResult),
            converter.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_SERIALIZE.getName(),
            TimeUnit.MILLISECONDS
        );
    }

    @Override
    public ActivityTypeRegistrationOptions getRegistrationOptions() {
        return registrationOptions;
    }

    @Override
    public ActivityTypeExecutionOptions getExecutionOptions() {
        return executionOptions;
    }

    void throwActivityFailureException(Throwable exception) throws ActivityFailureException, CancellationException {

        if (exception instanceof CancellationException) {
            throw (CancellationException) exception;
        }

        String reason = exception.getMessage();
        String details = null;
        try {
            details = ThreadLocalMetrics.getMetrics().recordSupplier(
                () -> converter.toData(exception),
                converter.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_SERIALIZE.getName(),
                TimeUnit.MILLISECONDS
            );
        }
        catch (DataConverterException dataConverterException) {
            if (dataConverterException.getCause() == null) {
                dataConverterException.initCause(exception);
            }
            throw dataConverterException;
        }

        if (details.length() > FlowValueConstraint.FAILURE_DETAILS.getMaxSize()) {
            log.warn("Length of details is over maximum input length of 32768. Actual details: " + details);
            Throwable truncatedException = WorkflowExecutionUtils.truncateStackTrace(exception);
            ThreadLocalMetrics.getMetrics().recordCount(MetricName.RESPONSE_TRUNCATED.getName(), 1,
                MetricName.getActivityTypeDimension(CurrentActivityExecutionContext.get().getTask().getActivityType()));
            details = ThreadLocalMetrics.getMetrics().recordSupplier(
                () -> converter.toData(truncatedException),
                converter.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_SERIALIZE.getName(),
                TimeUnit.MILLISECONDS
            );
        }

        throw new ActivityFailureException(reason, details);
    }

    public Method getMethod() {
        return activity;
    }

    public Object getActivitiesImplementation() {
        return activitiesImplementationObject;
    }
}
