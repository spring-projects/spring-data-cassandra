package org.springframework.cassandra.core.keyspace;

import org.springframework.cassandra.config.DataCenterReplication;
import org.springframework.cassandra.core.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.cassandra.core.util.MapBuilder;

public class CreateKeyspaceSpecification extends KeyspaceSpecification<CreateKeyspaceSpecification> {

	private boolean ifNotExists = false;

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

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API to create a keyspace. Convenient if imported
	 * statically.
	 */
	public static CreateKeyspaceSpecification createKeyspace() {
		return new CreateKeyspaceSpecification();
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
