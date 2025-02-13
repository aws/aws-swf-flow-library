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
package com.amazonaws.services.simpleworkflow.flow.aspectj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.simpleworkflow.flow.core.AndPromise;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;

import com.amazonaws.services.simpleworkflow.flow.DecisionContext;
import com.amazonaws.services.simpleworkflow.flow.DecisionContextProvider;
import com.amazonaws.services.simpleworkflow.flow.DecisionContextProviderImpl;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.WorkflowContext;
import com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetry;
import com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetryUpdate;
import com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetryVersion;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.interceptors.AsyncExecutor;
import com.amazonaws.services.simpleworkflow.flow.interceptors.AsyncRetryingExecutor;
import com.amazonaws.services.simpleworkflow.flow.interceptors.AsyncRunnable;
import com.amazonaws.services.simpleworkflow.flow.interceptors.ExponentialRetryPolicy;
import com.amazonaws.services.simpleworkflow.flow.worker.IncompatibleWorkflowDefinition;

/**
 * This class is for internal use only and may be changed or removed without
 * prior notice.
 */
@Aspect
public class ExponentialRetryAspect {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private final class DecoratorInvocationHandler implements AsyncRunnable {

        private final ProceedingJoinPoint pjp;

        private final Settable result;

        private AtomicLong firstAttemptTime;

        public DecoratorInvocationHandler(ProceedingJoinPoint pjp, AtomicLong firstAttemptTime, Settable result) {
            this.pjp = pjp;
            this.firstAttemptTime = firstAttemptTime;
            this.result = result;
        }

        @Override
        public void run() throws Throwable {
            List<Promise> waitFors = new ArrayList<>();
            if (pjp.getArgs() != null) {
                for (Object arg : pjp.getArgs()) {
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
                    firstAttemptTime.compareAndSet(0, decisionContextProviderImpl.getDecisionContext().getWorkflowClock().currentTimeMillis());
                }
            };
            if (result != null) {
                result.unchain();
                result.chain((Promise) pjp.proceed());
            }
            else {
                pjp.proceed();
            }
        }
    }

    private final DecisionContextProvider decisionContextProviderImpl = new DecisionContextProviderImpl();

    @Around("execution(@com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetryUpdate * *(..)) && @annotation(retryUpdateAnnotation)")
    public Object retry(final ProceedingJoinPoint pjp, ExponentialRetryUpdate retryUpdateAnnotation) throws Throwable {
        ExponentialRetryVersion[] versions = retryUpdateAnnotation.versions();
        TreeMap<Integer, ExponentialRetry> retriesByVersion = new TreeMap<Integer, ExponentialRetry>();
        for (ExponentialRetryVersion retryVersion : versions) {
            int internalVersion = retryVersion.implementationVersion();
            ExponentialRetry previous = retriesByVersion.put(internalVersion, retryVersion.retry());
            if (previous != null) {
                throw new IncompatibleWorkflowDefinition("More then one @ExponentialRetry annotation found for version "
                        + internalVersion);
            }
        }
        DecisionContext decisionContext = decisionContextProviderImpl.getDecisionContext();
        WorkflowContext workflowContext = decisionContext.getWorkflowContext();
        String component = retryUpdateAnnotation.component();
        // Checking isImplementationVersion from the largest version to the lowest one
        for (int internalVersion : retriesByVersion.descendingKeySet()) {
            ExponentialRetry retryAnnotation = retriesByVersion.get(internalVersion);
            if (workflowContext.isImplementationVersion(component, internalVersion)) {
                return retry(pjp, retryAnnotation);
            }
        }
        throw new IncompatibleWorkflowDefinition(
                "@ExponentialRetryUpdate doesn't include implementationVersion compatible with the workflow execution");
    }

    @Around("execution(@com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetry * *(..)) && @annotation(retryAnnotation)")
    public Object retry(final ProceedingJoinPoint pjp, ExponentialRetry retryAnnotation) throws Throwable {
        ExponentialRetryPolicy retryPolicy = createExponentialRetryPolicy(retryAnnotation);

        WorkflowClock clock = decisionContextProviderImpl.getDecisionContext().getWorkflowClock();
        AtomicLong firstAttemptTime = new AtomicLong(0);
        AsyncExecutor executor = new AsyncRetryingExecutor(retryPolicy, clock, firstAttemptTime);

        Settable<?> result;
        if (isVoidReturnType(pjp)) {
            result = null;
        }
        else {
            result = new Settable<Object>();
        }
        DecoratorInvocationHandler handler = new DecoratorInvocationHandler(pjp, firstAttemptTime, result);
        executor.execute(handler);
        return result;
    }

    private boolean isVoidReturnType(final ProceedingJoinPoint pjp) {
        boolean isVoidReturnType = false;
        final Signature signature = pjp.getStaticPart().getSignature();
        if (signature instanceof MethodSignature) {
            final MethodSignature methodSignature = (MethodSignature) signature;
            isVoidReturnType = (methodSignature != null) ? Void.TYPE.equals(methodSignature.getReturnType()) : false;
        }
        return isVoidReturnType;
    }

    private ExponentialRetryPolicy createExponentialRetryPolicy(ExponentialRetry retryAnnotation) {

        ExponentialRetryPolicy retryPolicy = new ExponentialRetryPolicy(retryAnnotation.initialRetryIntervalSeconds()).withExceptionsToRetry(
                Arrays.asList(retryAnnotation.exceptionsToRetry())).withExceptionsToExclude(
                Arrays.asList(retryAnnotation.excludeExceptions())).withBackoffCoefficient(retryAnnotation.backoffCoefficient()).withMaximumRetryIntervalSeconds(
                retryAnnotation.maximumRetryIntervalSeconds()).withRetryExpirationIntervalSeconds(
                retryAnnotation.retryExpirationSeconds()).withMaximumAttempts(retryAnnotation.maximumAttempts());

        retryPolicy.validate();
        return retryPolicy;
    }
}
