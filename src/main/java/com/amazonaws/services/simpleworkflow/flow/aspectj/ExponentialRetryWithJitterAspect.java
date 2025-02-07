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
package com.amazonaws.services.simpleworkflow.flow.aspectj;

import com.amazonaws.services.simpleworkflow.flow.DecisionContextProvider;
import com.amazonaws.services.simpleworkflow.flow.DecisionContextProviderImpl;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClock;
import com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetryWithJitter;
import com.amazonaws.services.simpleworkflow.flow.core.AndPromise;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;
import com.amazonaws.services.simpleworkflow.flow.core.Task;
import com.amazonaws.services.simpleworkflow.flow.interceptors.AsyncExecutor;
import com.amazonaws.services.simpleworkflow.flow.interceptors.AsyncRetryingExecutor;
import com.amazonaws.services.simpleworkflow.flow.interceptors.AsyncRunnable;
import com.amazonaws.services.simpleworkflow.flow.interceptors.ExponentialRetryWithJitterPolicy;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * See {@link com.amazonaws.services.simpleworkflow.flow.aspectj.ExponentialRetryAspect}
 * for reference.
 */
@Aspect
public class ExponentialRetryWithJitterAspect {

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
            } else {
                pjp.proceed();
            }
        }
    }

    private final DecisionContextProvider decisionContextProviderImpl = new DecisionContextProviderImpl();

    @Around("execution(@com.amazonaws.services.simpleworkflow.flow.annotations.ExponentialRetryWithJitter * *(..)) && @annotation(exponentialRetryWithJitterAnnotation)")
    public Object retry(final ProceedingJoinPoint pjp, final ExponentialRetryWithJitter exponentialRetryWithJitterAnnotation) throws Throwable {

        ExponentialRetryWithJitterPolicy retryPolicy = createExponentialRetryWithJitterPolicy(exponentialRetryWithJitterAnnotation);
        WorkflowClock clock = decisionContextProviderImpl.getDecisionContext().getWorkflowClock();
        AtomicLong firstAttemptTime = new AtomicLong(0);
        AsyncExecutor executor = new AsyncRetryingExecutor(retryPolicy, clock, firstAttemptTime);

        Settable<?> result;

        if (isVoidReturnType(pjp)) {
            result = null;
        } else {
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

    private ExponentialRetryWithJitterPolicy createExponentialRetryWithJitterPolicy(final ExponentialRetryWithJitter exponentialRetryWithJitterAnnotation) {

        final String currentRunId = decisionContextProviderImpl.getDecisionContext().getWorkflowContext().getWorkflowExecution().getRunId();
        final Random jitterCoefficientGenerator = new Random(currentRunId.hashCode());
        final ExponentialRetryWithJitterPolicy retryPolicy = new ExponentialRetryWithJitterPolicy(
                exponentialRetryWithJitterAnnotation.initialRetryIntervalSeconds(),
                jitterCoefficientGenerator,
                exponentialRetryWithJitterAnnotation.maxJitterCoefficient());

        retryPolicy.setBackoffCoefficient(exponentialRetryWithJitterAnnotation.backoffCoefficient());
        retryPolicy.setExceptionsToExclude(Arrays.asList(exponentialRetryWithJitterAnnotation.excludeExceptions()));
        retryPolicy.setExceptionsToRetry(Arrays.asList(exponentialRetryWithJitterAnnotation.exceptionsToRetry()));
        retryPolicy.setMaximumAttempts(exponentialRetryWithJitterAnnotation.maximumAttempts());
        retryPolicy.setMaximumRetryIntervalSeconds(exponentialRetryWithJitterAnnotation.maximumRetryIntervalSeconds());
        retryPolicy.setRetryExpirationIntervalSeconds(exponentialRetryWithJitterAnnotation.retryExpirationSeconds());
        retryPolicy.validate();
        return retryPolicy;	
    }

}