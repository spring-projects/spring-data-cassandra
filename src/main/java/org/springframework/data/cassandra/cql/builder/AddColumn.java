package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.noNull;

import com.datastax.driver.core.DataType;

public class AddColumn extends ColumnTypeChange {

	public AddColumn(String name, DataType type) {
		super(name, type);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("ADD ").append(getNameAsIdentifier()).append(" TYPE ").append(getType().getName());
	}
}
