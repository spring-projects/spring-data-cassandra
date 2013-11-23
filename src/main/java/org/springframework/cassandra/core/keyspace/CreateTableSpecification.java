package org.springframework.cassandra.core.keyspace;

/**
 * Builder class to construct a <code>CREATE TABLE</code> specification.
 * 
 * @author Matthew T. Adams
 */
public class CreateTableSpecification extends TableSpecification<CreateTableSpecification> {

	private boolean ifNotExists = false;

	/**
	 * Causes the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateTableSpecification ifNotExists() {
		return ifNotExists(true);
	}

	/**
	 * Toggles the inclusion of an <code>IF NOT EXISTS</code> clause.
	 * 
	 * @return this
	 */
	public CreateTableSpecification ifNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
		return this;
	}

	public boolean getIfNotExists() {
		return ifNotExists;
	}
}
