package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.cassandra.core.cql.CqlStringUtils.identifize;

public abstract class ColumnChangeSpecification {

	private String name;

	public ColumnChangeSpecification(String name) {
		setName(name);
	}

	private void setName(String name) {
		checkIdentifier(name);
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public String getNameAsIdentifier() {
		return identifize(name);
	}
}
