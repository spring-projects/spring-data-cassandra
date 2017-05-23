/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.integration.support;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

/**
 * Convenient listener base class that includes a {@link CountDownLatch} in order to test asynchronous behavior. This
 * class can be extended
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public abstract class CallbackSynchronizationSupport {

	private final CountDownLatch latch;

	/**
	 * Create a new {@link CallbackSynchronizationSupport}
	 */
	protected CallbackSynchronizationSupport() {
		this(1);
	}

	/**
	 * Create a new {@link CallbackSynchronizationSupport} for a given {@code latchCount} of callbacks.
	 *
	 * @param latchCount {@link CallbackSynchronizationSupport} for a given {@code latchCount} of callbacks
	 */
	protected CallbackSynchronizationSupport(int latchCount) {
		latch = new CountDownLatch(latchCount);
	}

	/**
	 * Await results without a timeout.
	 *
	 * @throws InterruptedException
	 */
	public final void await() throws InterruptedException {
		latch.await();
	}

	/**
	 * Await the results with a timeout.
	 *
	 * @param timeout must be greater or equal to 0
	 * @param timeUnit must not be {@literal null}.
	 * @throws InterruptedException
	 */
	public final void await(long timeout, TimeUnit timeUnit) throws InterruptedException {

		Assert.isTrue(timeout >= 0, "Timeout must be greater or equal to 0");
		Assert.notNull(timeUnit, "TimeUnit must not be null");

		latch.await(timeout, timeUnit);
	}

	/**
	 * Indicate an incoming event and count down the latch by {@literal 1}.
	 */
	protected final void countDown() {
		latch.countDown();
	}
}
