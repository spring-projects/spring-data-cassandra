/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.keyspace;

import org.springframework.data.cassandra.core.cql.KeyspaceIdentifier;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.data.cassandra.util.MapBuilder;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Object to configure a {@code CREATE KEYSPACE} specification.
 *
 * @author Mark Paluch
 */
public class CreateKeyspaceSpecification extends KeyspaceOptionsSpecification<CreateKeyspaceSpecification>
		implements KeyspaceDescriptor {

	private boolean ifNotExists = false;

	private CreateKeyspaceSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API given {@code name} to create a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link CreateKeyspaceSpecification}.
	 */
	public static CreateKeyspaceSpecification createKeyspace(String name) {
		return new CreateKeyspaceSpecification(CqlIdentifier.fromCql(name));
	}

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API given {@code name} to create a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null}.
	 * @return a new {@link CreateKeyspaceSpecification}.
	 * @deprecated since 3.0, use {@link #createKeyspace(CqlIdentifier)}
	 */
	@Deprecated
	public static CreateKeyspaceSpecification createKeyspace(KeyspaceIdentifier name) {
		return new CreateKeyspaceSpecification(CqlIdentifier.fromCql(name.toCql()));
	}

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API given {@code name} to create a keyspace.
	 * Convenient if imported statically.
	 *
	 * @param name must not be {@literal null}.
	 * @return a new {@link CreateKeyspaceSpecification}.
	 * @since 3.0
	 */
	public static CreateKeyspaceSpecification createKeyspace(CqlIdentifier name) {
		return new CreateKeyspaceSpecification(name);
	}

	/**
	 * Causes the inclusion of an {@code IF NOT EXISTS} clause.
	 *
	 * @return this
	 */
	public CreateKeyspaceSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an {@code IF NOT EXISTS} clause.
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

	/**
	 * Configure simple replication with a replication factor of {@code 1}.
	 *
	 * @return this.
	 */
	public CreateKeyspaceSpecification withSimpleReplication() {
		return withSimpleReplication(1);
	}

	/**
	 * Configure simple replication with a {@code replicationFactor}.
	 *
	 * @return this.
	 */
	public CreateKeyspaceSpecification withSimpleReplication(long replicationFactor) {

		return with(KeyspaceOption.REPLICATION,
				MapBuilder.map(Option.class, Object.class)
						.entry(new DefaultOption("class", String.class, true, false, true),
								ReplicationStrategy.SIMPLE_STRATEGY.getValue())
						.entry(new DefaultOption("replication_factor", Long.class, true, false, false), replicationFactor).build());
	}

	/**
	 * Configure datacenter replication given {@link DataCenterReplication}.
	 *
	 * @return this.
	 */
	public CreateKeyspaceSpecification withNetworkReplication(DataCenterReplication... dcrs) {

		MapBuilder<Option, Object> builder = MapBuilder.map(Option.class, Object.class).entry(
				new DefaultOption("class", String.class, true, false, true),
				ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY.getValue());

		for (DataCenterReplication dcr : dcrs) {
			builder.entry(new DefaultOption(dcr.getDataCenter(), Long.class, true, false, false), dcr.getReplicationFactor());
		}

		return with(KeyspaceOption.REPLICATION, builder.build());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOptionsSpecification#with(org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption)
	 */
	@Override
	public CreateKeyspaceSpecification with(KeyspaceOption option) {
		return super.with(option);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOptionsSpecification#with(org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption, java.lang.Object)
	 */
	@Override
	public CreateKeyspaceSpecification with(KeyspaceOption option, Object value) {
		return super.with(option, value);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOptionsSpecification#with(java.lang.String, java.lang.Object, boolean, boolean)
	 */
	@Override
	public CreateKeyspaceSpecification with(String name, @Nullable Object value, boolean escape, boolean quote) {
		return super.with(name, value, escape, quote);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof CreateKeyspaceSpecification)) {
			return false;
		}

		if (!super.equals(o)) {
			return false;
		}

		CreateKeyspaceSpecification that = (CreateKeyspaceSpecification) o;
		return ifNotExists == that.ifNotExists;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (ifNotExists ? 1 : 0);
		return result;
	}
}
