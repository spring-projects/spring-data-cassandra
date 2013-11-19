package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.data.cassandra.cql.CqlStringUtils.noNull;
import static org.springframework.data.cassandra.cql.CqlStringUtils.identifize;

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
