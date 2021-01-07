/*
 * Copyright 2017-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.keyspace;

import java.util.Map;

import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.data.cassandra.util.MapBuilder;

/**
 * Keyspace attributes.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class KeyspaceAttributes {

	public static final ReplicationStrategy DEFAULT_REPLICATION_STRATEGY = ReplicationStrategy.SIMPLE_STRATEGY;
	public static final long DEFAULT_REPLICATION_FACTOR = 1;
	public static final boolean DEFAULT_DURABLE_WRITES = true;

	private ReplicationStrategy replicationStrategy = DEFAULT_REPLICATION_STRATEGY;
	private long replicationFactor = DEFAULT_REPLICATION_FACTOR;
	private boolean durableWrites = DEFAULT_DURABLE_WRITES;

	/**
	 * Returns a map of {@link Option}s suitable as the value of a {@link KeyspaceOption#REPLICATION} option with
	 * replication strategy class "SimpleStrategy" and with a replication factor of one.
	 */
	public static Map<Option, Object> newSimpleReplication() {
		return newSimpleReplication(DEFAULT_REPLICATION_FACTOR);
	}

	/**
	 * Returns a map of {@link Option}s suitable as the value of a {@link KeyspaceOption#REPLICATION} option with
	 * replication strategy class "SimpleStrategy" and with a replication factor equal to that given.
	 */
	public static Map<Option, Object> newSimpleReplication(long replicationFactor) {

		return MapBuilder.map(Option.class, Object.class)
				.entry(new DefaultOption("class", String.class, true, false, true),
						ReplicationStrategy.SIMPLE_STRATEGY.getValue())
				.entry(new DefaultOption("replication_factor", Long.class, true, false, false), replicationFactor).build();
	}

	/**
	 * Returns a map of {@link Option}s suitable as the value of a {@link KeyspaceOption#REPLICATION} option with
	 * replication strategy class "NetworkTopologyStrategy" and with data centers each with their corresponding
	 * replication factors.
	 */
	public static Map<Option, Object> newNetworkReplication(DataCenterReplication... dataCenterReplications) {

		MapBuilder<Option, Object> builder = MapBuilder.map(Option.class, Object.class).entry(
				new DefaultOption("class", String.class, true, false, true),
				ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.getValue());

		for (DataCenterReplication dcr : dataCenterReplications) {
			builder.entry(new DefaultOption(dcr.getDataCenter(), Long.class, true, false, false), dcr.getReplicationFactor());
		}

		return builder.build();
	}

	public ReplicationStrategy getReplicationStrategy() {
		return replicationStrategy;
	}

	public void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
		this.replicationStrategy = replicationStrategy;
	}

	public long getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(long replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public boolean getDurableWrites() {
		return durableWrites;
	}

	public void setDurableWrites(boolean durableWrites) {
		this.durableWrites = durableWrites;
	}
}
