package org.springframework.data.cassandra.cql.builder;

import static org.springframework.data.cassandra.cql.CqlStringUtils.noNull;

import com.datastax.driver.core.DataType;

public class AlterColumn extends ColumnTypeChange {

	public AlterColumn(String name, DataType type) {
		super(name, type);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("ALTER ").append(getNameAsIdentifier()).append(" TYPE ")
				.append(getType().getName());
	}
}
