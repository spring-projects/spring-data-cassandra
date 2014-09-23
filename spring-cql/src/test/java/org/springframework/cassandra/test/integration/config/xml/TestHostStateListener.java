/*
 * Copyright 2013-2014 the original author or authors.
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
import com.datastax.driver.core.Host.StateListener;

/**
 * @author David Webb
 */
public class TestHostStateListener implements StateListener {

	private final static Logger log = LoggerFactory.getLogger(TestHostStateListener.class);

	@Override
	public void onAdd(Host host) {
		log.info("Host Added: " + host.getAddress());
	}

	@Override
	public void onUp(Host host) {
		log.info("Host Up: " + host.getAddress());
	}

	@Override
	public void onDown(Host host) {
		log.info("Host Down: " + host.getAddress());
	}

	@Override
	public void onRemove(Host host) {
		log.info("Host Removed: " + host.getAddress());
	}

	@Override
	public void onSuspected(Host host) {
		log.info("Host Suspected: " + host.getAddress());
	}

}
