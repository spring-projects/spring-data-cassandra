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
package org.springframework.cassandra.core.keyspace;

import org.springframework.cassandra.config.DataCenterReplication;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.cql.KeyspaceIdentifier;
import org.springframework.cassandra.core.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.cassandra.core.util.MapBuilder;

public class CreateKeyspaceSpecification extends KeyspaceSpecification<CreateKeyspaceSpecification> {

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API to create a keyspace. Convenient if imported
	 * statically.
	 */
	public static CreateKeyspaceSpecification createKeyspace() {
		return new CreateKeyspaceSpecification();
	}

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API to create a keyspace. Convenient if imported
	 * statically.
	 */
	public static CreateKeyspaceSpecification createKeyspace(String name) {
		return new CreateKeyspaceSpecification(name);
	}

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API to create a keyspace. Convenient if imported
	 * statically.
	 */
	public static CreateKeyspaceSpecification createKeyspace(KeyspaceIdentifier name) {
		return new CreateKeyspaceSpecification(name);
	}

	private boolean ifNotExists = false;

	public CreateKeyspaceSpecification() {}

	public CreateKeyspaceSpecification(String name) {
		name(name);
	}

	public CreateKeyspaceSpecification(KeyspaceIdentifier name) {
		name(name);
	}

	/**
	 * Causes the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateKeyspaceSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateKeyspaceSpecification ifNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
		return this;
	}

	public boolean getIfNotExists() {
		return ifNotExists;
	}

	public CreateKeyspaceSpecification withSimpleReplication() {
		return withSimpleReplication(1);
	}

	public CreateKeyspaceSpecification withSimpleReplication(long replicationFactor) {
		return with(
				KeyspaceOption.REPLICATION,
				MapBuilder
						.map(Option.class, Object.class)
						.entry(new DefaultOption("class", String.class, true, false, true),
								ReplicationStrategy.SIMPLE_STRATEGY.getValue())
						.entry(new DefaultOption("replication_factor", Long.class, true, false, false), replicationFactor).build());
	}

	public CreateKeyspaceSpecification withNetworkReplication(DataCenterReplication... dcrs) {

		MapBuilder<Option, Object> builder = MapBuilder.map(Option.class, Object.class).entry(
				new DefaultOption("class", String.class, true, false, true),
				ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.getValue());

		for (DataCenterReplication dcr : dcrs) {
			builder.entry(new DefaultOption(dcr.dataCenter, Long.class, true, false, false), dcr.replicationFactor);
		}

		return with(KeyspaceOption.REPLICATION, builder.build());
	}

	@Override
	public CreateKeyspaceSpecification name(String name) {
		return (CreateKeyspaceSpecification) super.name(name);
	}

	@Override
	public CreateKeyspaceSpecification name(KeyspaceIdentifier name) {
		return (CreateKeyspaceSpecification) super.name(name);
	}

	@Override
	public CreateKeyspaceSpecification with(KeyspaceOption option) {
		return (CreateKeyspaceSpecification) super.with(option);
	}

	@Override
	public CreateKeyspaceSpecification with(KeyspaceOption option, Object value) {
		return (CreateKeyspaceSpecification) super.with(option, value);
	}

	@Override
	public CreateKeyspaceSpecification with(String name, Object value, boolean escape, boolean quote) {
		return (CreateKeyspaceSpecification) super.with(name, value, escape, quote);
	}
}
