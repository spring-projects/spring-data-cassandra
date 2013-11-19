package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.noNull;

public class DropColumn extends ColumnChange {

	public DropColumn(String name) {
		super(name);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("DROP ").append(getNameAsIdentifier());
	}
}
