/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.config;

/**
 * Keyspace attributes.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class KeyspaceAttributes {

	public static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
	public static final int DEFAULT_REPLICATION_FACTOR = 1;
	public static final boolean DEFAULT_DURABLE_WRITES = true;

	private String replicationStrategy = DEFAULT_REPLICATION_STRATEGY;
	private int replicationFactor = DEFAULT_REPLICATION_FACTOR;
	private boolean durableWrites = DEFAULT_DURABLE_WRITES;

	public String getReplicationStrategy() {
		return replicationStrategy;
	}

	public void setReplicationStrategy(String replicationStrategy) {
		this.replicationStrategy = replicationStrategy;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public boolean isDurableWrites() {
		return durableWrites;
	}

	public void setDurableWrites(boolean durableWrites) {
		this.durableWrites = durableWrites;
	}
}
