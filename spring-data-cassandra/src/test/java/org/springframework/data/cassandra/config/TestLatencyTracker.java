/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.Statement;

/**
 * {@link LatencyTracker} that logs latency events and their payload. This class can be considered a test dummy and is
 * suitable for mocking.
 *
 * @author David Webb
 * @author Oliver Gierke
 * @author Antoine Toulme
 */
public class TestLatencyTracker implements LatencyTracker {

	private static final Logger LOG = LoggerFactory.getLogger(TestLatencyTracker.class);

	@Override
	public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
		LOG.info("Latency Tracker update: {}, {} nanoseconds.", host.getAddress(), newLatencyNanos);
	}

	@Override
	public void onRegister(Cluster cluster) {
		LOG.info("Latency Tracker onRegister: {}", cluster);
	}

	@Override
	public void onUnregister(Cluster cluster) {
		LOG.info("Latency Tracker onUnregister: {}", cluster);
	}
}
