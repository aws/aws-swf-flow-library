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

public abstract class TryFinally extends TryCatchFinally {

	public TryFinally(Promise<?>... waitFor) {
	    // The reason this() is not called here is to pass correct value of the skipStackLines.
	    // While this() passes the same value it also adds its own line into the stack trace.
	    super(null, null, 7, waitFor);
	}

	public TryFinally(boolean daemon, Promise<?>... waitFor) {
	    super(daemon, null, 7, waitFor);
	}
	
	public TryFinally(AsyncContextAware parent, boolean daemon, Promise<?>... waitFor) {
        super(parent, daemon, null, 7, waitFor);
    }

    public TryFinally(AsyncContextAware parent, Promise<?>... waitFor) {
        super(parent, null, null, 7, waitFor);
    }

    @Override
	protected void doCatch(Throwable e) throws Throwable {
		throw e;
	}

}
