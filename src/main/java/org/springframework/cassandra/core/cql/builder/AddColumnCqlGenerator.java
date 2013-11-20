package org.springframework.cassandra.core.cql.builder;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.AddColumnSpecification;

public class AddColumnCqlGenerator extends ColumnChangeCqlGenerator<AddColumnSpecification> {

	public AddColumnCqlGenerator(AddColumnSpecification specification) {
		super(specification);
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("ADD ").append(spec().getNameAsIdentifier()).append(" TYPE ")
				.append(spec().getType().getName());
	}
}
