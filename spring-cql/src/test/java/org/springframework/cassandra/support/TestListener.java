/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cassandra.support;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Convenient listener base class that includes a {@link CountDownLatch} in order to test asynchronous behavior.
 *
 * @author Matthew T. Adams
 */
public class TestListener {

	protected CountDownLatch latch;

	public TestListener() {
		this(1);
	}

	public TestListener(int latchCount) {
		latch = new CountDownLatch(latchCount);
	}

	public void await() throws InterruptedException {
		latch.await();
	}

	public void await(long ms) throws InterruptedException {
		latch.await(ms, TimeUnit.MILLISECONDS);
	}

	public void countDown() {
		latch.countDown();
	}
}
