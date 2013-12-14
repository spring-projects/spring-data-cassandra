package org.springframework.cassandra.core.keyspace;

public class DropKeyspaceSpecification extends KeyspaceNameSpecification<DropKeyspaceSpecification> {

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

}
