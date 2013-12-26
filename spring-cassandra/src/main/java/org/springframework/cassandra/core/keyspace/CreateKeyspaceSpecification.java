package org.springframework.cassandra.core.keyspace;

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

}
