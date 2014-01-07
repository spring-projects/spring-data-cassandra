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
