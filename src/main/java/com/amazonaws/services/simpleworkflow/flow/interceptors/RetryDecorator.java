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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.simpleworkflow.flow.DecisionContextProviderImpl;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.core.AndPromise;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;

/**
 * In case of failures repeats every call to a wrapped object method according
 * the provided {@link RetryPolicy}.
 * 
 * @author fateev
 */
public class RetryDecorator implements Decorator {

    private final RetryPolicy retryPolicy;

    private final WorkflowClock clock;

    public RetryDecorator(RetryPolicy retryPolicy, WorkflowClock clock) {
        this.retryPolicy = retryPolicy;
        this.clock = clock;
    }

    public RetryDecorator(RetryPolicy retryPolicy) {
        this(retryPolicy, new DecisionContextProviderImpl().getDecisionContext().getWorkflowClock());
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <V> V decorate(Class<V> interfaces, V object) {

        Class<?>[] interfazes = { interfaces };

        return (V) Proxy.newProxyInstance(object.getClass().getClassLoader(), interfazes, new DecoratorInvocationHandler(object));

    }

    @SuppressWarnings("unchecked")
    @Override
    public final <V> V decorate(Class<?>[] interfaces, V object) {

        return (V) Proxy.newProxyInstance(object.getClass().getClassLoader(), interfaces, new DecoratorInvocationHandler(object));
    }

    private final class DecoratorInvocationHandler implements InvocationHandler {

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private final class RetriedRunnable implements AsyncRunnable {

            private final Object[] args;

            private final Method method;

            private final AtomicLong firstAttemptTime;

            private Settable result;

            private RetriedRunnable(Object[] args, Method method, AtomicLong firstAttemptTime) {
                this.args = args;
                Class<?> returnType = method.getReturnType();
                boolean voidReturnType = Void.TYPE.equals(returnType);
                if (!voidReturnType) {
                    if (!Promise.class.isAssignableFrom(returnType)) {
                        throw new IllegalArgumentException("Cannot decorate " + method.getName()
                                + " as its return type is not void or Promise");
                    }
                    result = new Settable();
                }
                this.method = method;
                this.firstAttemptTime = firstAttemptTime;
            }

            @Override
            public void run() throws Throwable {
                List<Promise> waitFors = new ArrayList<>();
                if (args != null) {
                    for (Object arg : args) {
                        if (arg instanceof Promise) {
                            waitFors.add((Promise) arg);
                        }
                        if (arg instanceof Promise[]) {
                            waitFors.addAll(Arrays.asList((Promise[]) arg));
                        }
                    }
                }
                AndPromise waitFor = new AndPromise(waitFors);
                new Task(waitFor) {
                    @Override
                    protected void doExecute() throws Throwable {
                        firstAttemptTime.compareAndSet(0, new DecisionContextProviderImpl().getDecisionContext().getWorkflowClock().currentTimeMillis());
                    }
                };
                if (result == null) {
                    // void return type
                    method.invoke(object, args);
                }
                else {
                    // Need to unchain as it could be chained to the result of the previous run invocation
                    result.unchain();
                    result.chain((Promise) method.invoke(object, args));
                }
            }

            public Promise getResult() {
                return result;
            }

        }

        private final Object object;

        public DecoratorInvocationHandler(Object object) {
            this.object = object;
        }

        @Override
        public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
            try {
                if (!isDecorated(method, args)) {
                    return method.invoke(object, args);
                }
            }
            catch (InvocationTargetException ite) {
                throw ite.getTargetException();
            }

            AtomicLong firstAttemptTime = new AtomicLong(0);
            RetriedRunnable command = new RetriedRunnable(args, method, firstAttemptTime);
            AsyncRetryingExecutor executor = new AsyncRetryingExecutor(retryPolicy, clock, firstAttemptTime);
            executor.execute(command);
            return command.getResult();
        }
    }

    protected boolean isDecorated(Method method, Object[] args) {
        return !method.getDeclaringClass().equals(Object.class);
    }
}
