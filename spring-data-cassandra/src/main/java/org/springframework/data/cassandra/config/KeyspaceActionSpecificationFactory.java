/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.cassandra.config;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.cassandra.core.cql.KeyspaceIdentifier;
import org.springframework.data.cassandra.core.cql.keyspace.AlterKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DataCenterReplication;
import org.springframework.data.cassandra.core.cql.keyspace.DefaultOption;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.data.cassandra.core.cql.keyspace.Option;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Factory to create {@link CreateKeyspaceSpecification} and {@link DropKeyspaceSpecification}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class KeyspaceActionSpecificationFactory {

	private final CqlIdentifier name;

	private final List<DataCenterReplication> replications;

	private final ReplicationStrategy replicationStrategy;

	private final long replicationFactor;

	private final boolean durableWrites;

	KeyspaceActionSpecificationFactory(CqlIdentifier name, List<DataCenterReplication> replications,
			ReplicationStrategy replicationStrategy, long replicationFactor, boolean durableWrites) {

		this.name = name;
		this.replications = replications;
		this.replicationStrategy = replicationStrategy;
		this.replicationFactor = replicationFactor;
		this.durableWrites = durableWrites;
	}

	/**
	 * Create a new {@link KeyspaceActionSpecificationFactoryBuilder} to configure a new
	 * {@link KeyspaceActionSpecificationFactory}.
	 *
	 * @param keyspaceName must not be {@literal null} or empty.
	 * @return the new {@link KeyspaceActionSpecificationFactoryBuilder} for {@code keyspaceName}.
	 */
	public static KeyspaceActionSpecificationFactoryBuilder builder(String keyspaceName) {
		return builder(CqlIdentifier.fromCql(keyspaceName));
	}

	/**
	 * Create a new {@link KeyspaceActionSpecificationFactoryBuilder} to configure a new
	 * {@link KeyspaceActionSpecificationFactory}.
	 *
	 * @param keyspaceName must not be {@literal null} or empty.
	 * @return the new {@link KeyspaceActionSpecificationFactoryBuilder} for {@code keyspaceName}.
	 * @deprecated since 3.0, use {@link #builder(CqlIdentifier)}.
	 */
	@Deprecated
	public static KeyspaceActionSpecificationFactoryBuilder builder(KeyspaceIdentifier keyspaceName) {
		return builder(keyspaceName.toCqlIdentifier());
	}

	/**
	 * Create a new {@link KeyspaceActionSpecificationFactoryBuilder} to configure a new
	 * {@link KeyspaceActionSpecificationFactory}.
	 *
	 * @param keyspaceName must not be {@literal null} or empty.
	 * @return the new {@link KeyspaceActionSpecificationFactoryBuilder} for {@code keyspaceName}.
	 * @since 3.0
	 */
	public static KeyspaceActionSpecificationFactoryBuilder builder(CqlIdentifier keyspaceName) {
		return new KeyspaceActionSpecificationFactoryBuilder(keyspaceName);
	}

	/**
	 * Generate a {@link CreateKeyspaceSpecification} for the keyspace.
	 *
	 * @param ifNotExists {@literal true} to include {@code IF NOT EXISTS} rendering in the create statement.
	 * @return the {@link CreateKeyspaceSpecification}.
	 */
	public CreateKeyspaceSpecification create(boolean ifNotExists) {

		CreateKeyspaceSpecification create = CreateKeyspaceSpecification.createKeyspace(name).ifNotExists(ifNotExists)
				.with(KeyspaceOption.DURABLE_WRITES, durableWrites);

		Map<Option, Object> replication = getReplication();

		if (!replication.isEmpty()) {
			create.with(KeyspaceOption.REPLICATION, replication);
		}

		return create;
	}

	/**
	 * Generate a {@link AlterKeyspaceSpecification} for the keyspace.
	 *
	 * @return the {@link AlterKeyspaceSpecification}.
	 * @since 2.0.1
	 */
	public KeyspaceActionSpecification alter() {

		AlterKeyspaceSpecification alter = AlterKeyspaceSpecification.alterKeyspace(name)
				.with(KeyspaceOption.DURABLE_WRITES, durableWrites);

		Map<Option, Object> replication = getReplication();

		if (!replication.isEmpty()) {
			alter.with(KeyspaceOption.REPLICATION, replication);
		}

		return alter;
	}

	/**
	 * Create replication options represented as {@link Map}.
	 *
	 * @return the replication options represented as {@link Map}.
	 * @since 2.0.1
	 */
	protected Map<Option, Object> getReplication() {

		Map<Option, Object> replicationStrategyMap = new HashMap<>();

		if (hasReplicationOptions()) {

			replicationStrategyMap.put(new DefaultOption("class", String.class, true, false, true),
					replicationStrategy.getValue());

			if (replicationStrategy == ReplicationStrategy.SIMPLE_STRATEGY) {
				replicationStrategyMap.put(new DefaultOption("replication_factor", Long.class, true, false, false),
						replicationFactor);
			}

			if (replicationStrategy == ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY) {
				for (DataCenterReplication datacenter : replications) {
					replicationStrategyMap.put(new DefaultOption(datacenter.getDataCenter(), Long.class, true, false, false),
							datacenter.getReplicationFactor());
				}
			}
		}

		return replicationStrategyMap;
	}

	private boolean hasReplicationOptions() {

		if (replicationStrategy == ReplicationStrategy.SIMPLE_STRATEGY && replicationFactor > 0) {
			return true;
		}

		if (replicationStrategy == ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY && !replications.isEmpty()) {
			return true;
		}

		return false;
	}

	/**
	 * Generate a {@link DropKeyspaceSpecification} for the keyspace.
	 *
	 * @param ifExists {@literal true} to include {@code IF EXISTS} rendering in the drop statement.
	 * @return the {@link DropKeyspaceSpecification}.
	 */
	public DropKeyspaceSpecification drop(boolean ifExists) {
		return DropKeyspaceSpecification.dropKeyspace(name).ifExists(ifExists);
	}

	static class KeyspaceActionSpecificationFactoryBuilder {

		private final CqlIdentifier name;

		private final List<DataCenterReplication> replications = new ArrayList<>();

		private ReplicationStrategy replicationStrategy = ReplicationStrategy.SIMPLE_STRATEGY;

		private long replicationFactor;

		private boolean durableWrites = false;

		private KeyspaceActionSpecificationFactoryBuilder(CqlIdentifier name) {
			this.name = name;
		}

		/**
		 * Configure simple replication scheme for the keyspace action factory.
		 *
		 * @param replicationFactor the replication factor.
		 * @return this.
		 */
		KeyspaceActionSpecificationFactoryBuilder simpleReplication(int replicationFactor) {

			this.replicationFactor = replicationFactor;

			return replicationStrategy(ReplicationStrategy.SIMPLE_STRATEGY);
		}

		/**
		 * Configure datacenter replication scheme for the keyspace action factory.
		 *
		 * @param replication the replication configuration.
		 * @return this.
		 */
		KeyspaceActionSpecificationFactoryBuilder withDataCenter(DataCenterReplication replication) {

			replicationStrategy(ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY);
			this.replications.add(replication);
			return this;
		}

		private KeyspaceActionSpecificationFactoryBuilder replicationStrategy(ReplicationStrategy strategy) {

			this.replicationStrategy = strategy;
			return this;
		}

		/**
		 * Configure durable writes for the keyspace action factory.
		 *
		 * @param durableWrites {@literal true} to enable durable writes.
		 * @return this.
		 */
		KeyspaceActionSpecificationFactoryBuilder durableWrites(boolean durableWrites) {

			this.durableWrites = durableWrites;
			return this;
		}

		/**
		 * @return a new {@link KeyspaceActionSpecificationFactory}.
		 */
		public KeyspaceActionSpecificationFactory build() {

			return new KeyspaceActionSpecificationFactory(name, new ArrayList<>(replications), replicationStrategy,
					replicationFactor, durableWrites);
		}
	}
}
