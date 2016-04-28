/*
 * Copyright 2013-2015 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.integration.config.xml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.Statement;

/**
 * @author David Webb
 * @author Oliver Gierke
 */
public class TestLatencyTracker implements LatencyTracker {

	private final static Logger LOG = LoggerFactory.getLogger(TestLatencyTracker.class);

	@Override
	public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
		LOG.info("Latency Tracker: " + host.getAddress() + ", " + newLatencyNanos + " nanoseconds.");
	}

}
