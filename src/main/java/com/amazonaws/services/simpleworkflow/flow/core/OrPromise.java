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
package com.amazonaws.services.simpleworkflow.flow.core;

import java.util.Collection;

/**
 * Promise that becomes ready when any of its values becomes ready.
 * <code>null</code> value is considered ready.
 */
public class OrPromise extends Promise<Void> {

    private final class OrPromiseCallback implements Runnable {

        @Override
        public void run() {
            if (!impl.isReady()) {
                impl.set(null);
            }
        }
    }

    private static final Promise<?>[] EMPTY_VALUE_ARRAY = new Promise[0];

    private final Settable<Void> impl = new Settable<Void>();

    private final Promise<?>[] values;

    public OrPromise(Promise<?>... values) {
        this.values = values;
        if (values == null || values.length == 0) {
            impl.set(null);
        }
        Runnable callback = new OrPromiseCallback();
        for (Promise<?> value : values) {
            if (value != null) {
                value.addCallback(callback);
            }
            else {
                callback.run();
            }
        }
    }

    @SuppressWarnings({ "rawtypes" })
    public OrPromise(Collection<Promise> collection) {
        this(collection.toArray(EMPTY_VALUE_ARRAY));
    }

    public Promise<?>[] getValues() {
        return values;
    }

    @Override
    protected void addCallback(Runnable callback) {
        impl.addCallback(callback);
    }

    @Override
    public Void get() {
        return impl.get();
    }

    @Override
    public boolean isReady() {
        return impl.isReady();
    }

    @Override
    protected void removeCallback(Runnable callback) {
        impl.removeCallback(callback);
    }

}
