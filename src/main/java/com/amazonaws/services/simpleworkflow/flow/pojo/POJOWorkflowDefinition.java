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

import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.DataConverterException;
import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.WorkflowException;
import com.amazonaws.services.simpleworkflow.flow.common.FlowHelpers;
import com.amazonaws.services.simpleworkflow.flow.common.WorkflowExecutionUtils;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.TryCatch;
import com.amazonaws.services.simpleworkflow.flow.core.TryCatchFinally;
import com.amazonaws.services.simpleworkflow.flow.generic.WorkflowDefinition;
import com.amazonaws.services.simpleworkflow.flow.monitoring.MetricName;
import com.amazonaws.services.simpleworkflow.flow.monitoring.Metrics;
import com.amazonaws.services.simpleworkflow.flow.monitoring.ThreadLocalMetrics;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("rawtypes")
public class POJOWorkflowDefinition extends WorkflowDefinition {

    private final MethodConverterPair workflowMethod;

    private final Map<String, MethodConverterPair> signals;

    private final MethodConverterPair getStateMethod;

    private final Object workflowImplementationInstance;

    private final DataConverter converter;

    private final DecisionContext context;

    public POJOWorkflowDefinition(Object workflowImplmentationInstance, MethodConverterPair workflowImplementationMethod,
            Map<String, MethodConverterPair> signals, MethodConverterPair getStateMethod, DataConverter converter,
            DecisionContext context)
            throws ClassNotFoundException, SecurityException, NoSuchMethodException, NoSuchFieldException {
        this.workflowImplementationInstance = workflowImplmentationInstance;
        this.workflowMethod = workflowImplementationMethod;
        this.getStateMethod = getStateMethod;
        this.signals = signals;
        this.converter = converter;
        this.context = context;
    }

    @Override
    public Promise<String> execute(final String input) throws WorkflowException {
        final DataConverter c;
        if (workflowMethod.getConverter() == null) {
            c = converter;
        }
        else {
            c = workflowMethod.getConverter();
        }
        final Settable<String> result = new Settable<>();
        final AtomicReference<Promise> methodResult = new AtomicReference<>();
        new TryCatchFinally() {

            @Override
            protected void doTry() throws Throwable {

                //TODO: Support ability to call workflow using old client 
                // after new parameters were added to @Execute method
                // It requires creation of parameters array of the correct size and
                // populating the new parameter values with default values for each type
                final Object[] parameters = ThreadLocalMetrics.getMetrics().recordSupplier(
                    () -> c.fromData(input, Object[].class),
                    c.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_DESERIALIZE.getName(),
                    TimeUnit.MILLISECONDS
                );
                Method method = workflowMethod.getMethod();
                Object r = invokeMethod(method, parameters);
                if (!method.getReturnType().equals(Void.TYPE)) {
                    methodResult.set((Promise) r);
                }
            }

            @Override
            protected void doCatch(Throwable e) throws Throwable {
                // CancellationException can be caused by:
                //  1. cancellation request from a server (indicated by isCancelRequested returning true). 
                //     Should not be converted.
                //  2. being thrown by user code. Ut should be converted to WorkflowException as any other exception.
                //  3. being caused by exception from the sibling (signal handler). 
                //     In this case the exception cause is already WorkflowException. No double conversion necessary.
                if (!(e instanceof CancellationException)
                        || (!context.getWorkflowContext().isCancelRequested() && !(e.getCause() instanceof WorkflowException))) {
                    throwWorkflowException(c, e);
                }
            }

            @Override
            protected void doFinally() throws Throwable {
                Promise r = methodResult.get();
                if (r == null || r.isReady()) {
                    final Object workflowResult = r == null ? null : r.get();
                    result.set(
                        ThreadLocalMetrics.getMetrics().recordSupplier(
                            () -> c.toData(workflowResult),
                            c.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_SERIALIZE.getName(),
                            TimeUnit.MILLISECONDS
                        )
                    );
                }
            }
        };
        return result;
    }

    @Override
    public void signalRecieved(String signalName, String details) throws WorkflowException {
        MethodConverterPair signalMethod = signals.get(signalName);
        if (signalMethod != null) {
            DataConverter c = signalMethod.getConverter();
            if (c == null) {
                c = converter;
            }
            Method method = signalMethod.getMethod();
            final DataConverter delegatedConverter = c;
            final Object[] parameters = ThreadLocalMetrics.getMetrics().recordSupplier(
                () -> delegatedConverter.fromData(details, Object[].class),
                delegatedConverter.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_DESERIALIZE.getName(),
                TimeUnit.MILLISECONDS
            );
            new TryCatch() {

                @Override
                protected void doTry() throws Throwable {
                    invokeMethod(method, parameters);
                }

                @Override
                protected void doCatch(Throwable e) throws Throwable {
                    throwWorkflowException(delegatedConverter, e);
                    throw new IllegalStateException("Unreacheable");
                }

            };
        }
        else {
            // TODO: Unhandled signal
        }
    }

    @Override
    public String getWorkflowState() throws WorkflowException {
        if (getStateMethod == null) {
            return null;
        }
        final DataConverter c;
        if (getStateMethod.getConverter() == null) {
            c = converter;
        }
        else {
            c = getStateMethod.getConverter();
        }
        try {
            Method method = getStateMethod.getMethod();
            Object result = invokeMethod(method, null);
            return ThreadLocalMetrics.getMetrics().recordSupplier(
                () -> c.toData(result),
                c.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_SERIALIZE.getName(),
                TimeUnit.MILLISECONDS
            );
        }
        catch (Throwable e) {
            throwWorkflowException(c, e);
            throw new IllegalStateException("Unreacheable");
        }
    }

    private Object invokeMethod(final Method method, final Object[] input) throws Throwable {
        final Metrics metrics = ThreadLocalMetrics.getMetrics();
        final Metrics childMetrics = metrics.newMetrics();
        ThreadLocalMetrics.setCurrent(childMetrics);
        try {
            // Fill missing parameters with default values to make addition of new parameters backward compatible
            Object[] parameters = FlowHelpers.getInputParameters(method.getParameterTypes(), input);
            return method.invoke(workflowImplementationInstance, parameters);
        } catch (InvocationTargetException invocationException) {
            if (invocationException.getTargetException() != null) {
                throw invocationException.getTargetException();
            }
            throw invocationException;
        } finally {
            childMetrics.close();
            ThreadLocalMetrics.setCurrent(metrics);
        }
    }

    private void throwWorkflowException(DataConverter c, Throwable exception) throws WorkflowException {
        if (exception instanceof WorkflowException) {
            throw (WorkflowException) exception;
        }
        String reason = WorkflowExecutionUtils.truncateReason(exception.getMessage());
        String details;
        try {
            details = ThreadLocalMetrics.getMetrics().recordSupplier(
                () -> c.toData(exception),
                c.getClass().getSimpleName() + "@" + MetricName.Operation.DATA_CONVERTER_SERIALIZE.getName(),
                TimeUnit.MILLISECONDS
            );
        }
        catch (DataConverterException dataConverterException) {
            if (dataConverterException.getCause() == null) {
                dataConverterException.initCause(exception);
            }
            throw dataConverterException;
        }

        throw new WorkflowException(reason, details);
    }

    public Object getImplementationInstance() {
        return workflowImplementationInstance;
    }
}
