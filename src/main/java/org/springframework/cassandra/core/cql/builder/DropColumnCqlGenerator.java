package org.springframework.cassandra.core.cql.builder;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.DropColumnSpecification;

public class DropColumnCqlGenerator extends ColumnChangeCqlGenerator<DropColumnSpecification> {

	public DropColumnCqlGenerator(DropColumnSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("DROP ").append(spec().getNameAsIdentifier());
	}
}
