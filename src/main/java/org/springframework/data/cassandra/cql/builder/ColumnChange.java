package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.data.cassandra.cql.CqlStringUtils.identifize;

public abstract class ColumnChange {

	public abstract StringBuilder toCql(StringBuilder cql);

	private String name;

	public ColumnChange(String name) {
		setName(name);
	}

	public String toString() {
		return toCql(null).toString();
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
