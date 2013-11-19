package org.springframework.cassandra.core.cql.builder;

import static org.springframework.cassandra.core.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.cassandra.core.cql.CqlStringUtils.identifize;
import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

public class DropTableBuilder {

	private String name;
	private boolean ifExists;

	public DropTableBuilder name(String name) {
		checkIdentifier(name);
		this.name = name;
		return this;
	}

	public DropTableBuilder ifExists() {
		return ifExists(true);
	}

	public DropTableBuilder ifExists(boolean ifExists) {
		this.ifExists = ifExists;
		return this;
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("DROP TABLE ").append(ifExists ? "IF EXISTS " : "").append(identifize(name));
	}

	public String toCql() {
		return toCql(null).toString();
	}

	public String toString() {
		return toCql();
	}
}
