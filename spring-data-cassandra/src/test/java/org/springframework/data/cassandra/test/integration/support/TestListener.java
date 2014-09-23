package org.springframework.data.cassandra.test.integration.support;

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
