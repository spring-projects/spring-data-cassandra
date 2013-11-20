package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.cassandra.core.cql.CqlStringUtils.identifize;

public class DropTableSpecification {

	private String name;
	private boolean ifExists;

	public DropTableSpecification name(String name) {
		checkIdentifier(name);
		this.name = name;
		return this;
	}

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

	public String getNameAsIdentifier() {
		return identifize(name);
	}
}
