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
package com.amazonaws.services.simpleworkflow.flow.worker;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Semaphore that can be suspended. When suspended {@link #acquire()} operation
 * doesn't return permits even if available.
 * 
 * @author fateev
 */
class SuspendableSemaphore {

    private final Semaphore semaphore;

    private final Lock lock = new ReentrantLock();

    private final Condition suspentionCondition = lock.newCondition();

    private boolean suspended;

    public SuspendableSemaphore(int permits, boolean fair) {
        semaphore = new Semaphore(permits, fair);
    }

    public SuspendableSemaphore(int permits) {
        semaphore = new Semaphore(permits);
    }

    public void acquire() throws InterruptedException {
        semaphore.acquire();
        lock.lock();
        try {
            while (suspended) {
                suspentionCondition.await();
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void release() {
        semaphore.release();
    }

    public void suspend() {
        lock.lock();
        try {
            suspended = true;
        }
        finally {
            lock.unlock();
        }
    }

    public void resume() {
        lock.lock();
        try {
            suspended = false;
            suspentionCondition.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isSuspended() {
        lock.lock();
        try {
            return suspended;
        }
        finally {
            lock.unlock();
        }
    }

}
