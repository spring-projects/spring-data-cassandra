package org.springframework.cassandra.core.keyspace;

/**
 * Builder class that supports the construction of <code>DROP TABLE</code> specifications.
 * 
 * @author Matthew T. Adams
 */
public class DropTableSpecification extends TableNameSpecification<DropTableSpecification> {

	private boolean ifExists;

	public DropTableSpecification ifExists() {
		return ifExists(true);
	}

	public DropTableSpecification ifExists(boolean ifExists) {
		this.ifExists = ifExists;
		return this;
	}

	public boolean getIfExists() {
		return ifExists;
	}
}
