package org.springframework.cassandra.core.keyspace;

public class DropKeyspaceSpecification extends KeyspaceActionSpecification<DropKeyspaceSpecification> {

	private boolean ifExists;

	public DropKeyspaceSpecification ifExists() {
		return ifExists(true);
	}

	public DropKeyspaceSpecification ifExists(boolean ifExists) {
		this.ifExists = ifExists;
		return this;
	}

	public boolean getIfExists() {
		return ifExists;
	}

	/**
	 * Entry point into the {@link DropKeyspaceSpecification}'s fluent API to drop a keyspace. Convenient if imported
	 * statically.
	 */
	public static DropKeyspaceSpecification dropKeyspace() {
		return new DropKeyspaceSpecification();
	}

}
