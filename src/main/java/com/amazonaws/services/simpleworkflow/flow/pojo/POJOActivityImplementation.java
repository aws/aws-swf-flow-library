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
package com.amazonaws.services.simpleworkflow.flow.pojo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CancellationException;

import com.amazonaws.services.simpleworkflow.flow.ActivityExecutionContext;
import com.amazonaws.services.simpleworkflow.flow.ActivityFailureException;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.DataConverterException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.FlowValueConstraint;
import com.amazonaws.services.simpleworkflow.flow.generic.ActivityImplementationBase;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeExecutionOptions;
import com.amazonaws.services.simpleworkflow.flow.worker.ActivityTypeRegistrationOptions;
import com.amazonaws.services.simpleworkflow.flow.worker.CurrentActivityExecutionContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class POJOActivityImplementation extends ActivityImplementationBase {

    private static final Log log = LogFactory.getLog(POJOActivityImplementation.class);

    private final Method activity;

    private final Object activitiesImplmentationObject;

    private final ActivityTypeExecutionOptions executionOptions;

    private final DataConverter converter;

    private final ActivityTypeRegistrationOptions registrationOptions;

    public POJOActivityImplementation(Object activitiesImplmentationObject, Method activity,
            ActivityTypeRegistrationOptions registrationOptions, ActivityTypeExecutionOptions executionOptions,
            DataConverter converter) {
        this.activitiesImplmentationObject = activitiesImplmentationObject;
        this.activity = activity;
        this.registrationOptions = registrationOptions;
        this.executionOptions = executionOptions;
        this.converter = converter;
    }

    @Override
    protected String execute(String input, ActivityExecutionContext context)
            throws ActivityFailureException, CancellationException {
        Object[] inputParameters = converter.fromData(input, Object[].class);
        CurrentActivityExecutionContext.set(context);
        Object result = null;
        try {
            // Fill missing parameters with default values to make addition of new parameters backward compatible
            inputParameters = FlowHelpers.getInputParameters(activity.getParameterTypes(), inputParameters);
            result = activity.invoke(activitiesImplmentationObject, inputParameters);
        }
        catch (InvocationTargetException invocationException) {
            throwActivityFailureException(invocationException.getTargetException() != null ? invocationException.getTargetException()
                    : invocationException);
        }
        catch (IllegalArgumentException illegalArgumentException) {
            throwActivityFailureException(illegalArgumentException);
        }
        catch (IllegalAccessException illegalAccessException) {
            throwActivityFailureException(illegalAccessException);
        }
        finally {
            CurrentActivityExecutionContext.unset();
        }
        return converter.toData(result);
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
            details = converter.toData(exception);
        }
        catch (DataConverterException dataConverterException) {
            if (dataConverterException.getCause() == null) {
                dataConverterException.initCause(exception);
            }
            throw dataConverterException;
        }

        if (details.length() > FlowValueConstraint.FAILURE_DETAILS.getMaxSize()) {
            log.warn("Length of details is over maximum input length of 32768. Actual details: " + details);
            Throwable truncatedException = new Throwable(reason);
            truncatedException.setStackTrace(new StackTraceElement[] {exception.getStackTrace()[0]});
            details = converter.toData(truncatedException);
        }

        throw new ActivityFailureException(reason, details);
    }

    public Method getMethod() {
        return activity;
    }

    public Object getActivitiesImplementation() {
        return activitiesImplmentationObject;
    }
}
