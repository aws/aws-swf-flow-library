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
package com.amazonaws.services.simpleworkflow.flow.junit;

import java.util.List;

import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

/**
 * To be used instead of {@link BlockJUnit4ClassRunner} when testing
 * asynchronous code. Requires {@link WorkflowTest} rule (annotated with @Rule)
 * to be present in the tested class.
 * 
 * @author fateev
 */
public class FlowBlockJUnit4ClassRunner extends BlockJUnit4ClassRunner {

    private WorkflowTestBase workflowTestRule;

    private long timeout;

    private Class<? extends Throwable> expectedException;

    public FlowBlockJUnit4ClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected Statement withPotentialTimeout(FrameworkMethod method, final Object test, Statement next) {
        Test annotation = method.getAnnotation(Test.class);
        timeout = annotation.timeout();
        if (timeout > 0 && workflowTestRule != null) {
            workflowTestRule.setTestTimeoutActualTimeMilliseconds(timeout);
        }
        return next;
    }

    @Override
    protected List<MethodRule> rules(Object test) {
        List<MethodRule> result = super.rules(test);
        for (MethodRule methodRule : result) {
            if (WorkflowTestBase.class.isAssignableFrom(methodRule.getClass())) {
                workflowTestRule = (WorkflowTestBase) methodRule;
                workflowTestRule.setFlowTestRunner(true);
                if (timeout > 0) {
                    workflowTestRule.setTestTimeoutActualTimeMilliseconds(timeout);
                }
                workflowTestRule.setExpectedException(expectedException);
            }
        }
        return result;
    }

    @Override
    protected Statement possiblyExpectingExceptions(FrameworkMethod method, Object test, Statement next) {
        Test annotation = method.getAnnotation(Test.class);
        Class<? extends Throwable> expected = annotation.expected();

        if (expected == Test.None.class) {
            expectedException = null;
        } else {
            expectedException = expected;
        }

        if (workflowTestRule != null) {
            workflowTestRule.setExpectedException(expectedException);
        }

        return next;
    }

}
