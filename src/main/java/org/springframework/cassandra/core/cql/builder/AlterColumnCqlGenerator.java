package org.springframework.cassandra.core.cql.builder;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.AlterColumnSpecification;

public class AlterColumnCqlGenerator extends ColumnChangeCqlGenerator<AlterColumnSpecification> {

	public AlterColumnCqlGenerator(AlterColumnSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("ALTER ").append(spec().getNameAsIdentifier()).append(" TYPE ")
				.append(spec().getType().getName());
	}
}
