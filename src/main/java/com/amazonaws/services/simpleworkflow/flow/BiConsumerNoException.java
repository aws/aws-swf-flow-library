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
package com.amazonaws.services.simpleworkflow.flow;

import lombok.RequiredArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.function.BiConsumer;

@RequiredArgsConstructor
public class BiConsumerNoException<T, U> implements BiConsumer<T, U> {

    private static final Log log = LogFactory.getLog(BiConsumerNoException.class);

    private final BiConsumer<T, U> delegateBiConsumer;

    @Override
    public void accept(final T t, final U u) {
        try {
            delegateBiConsumer.accept(t, u);
        } catch (final RuntimeException e) {
            log.warn("Delegate BiConsumer failed: ", e);
        }
    }
}
