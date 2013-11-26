package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.AlterColumnSpecification;

/**
 * CQL generator for generating an <code>ALTER</code> column clause of an <code>ALTER TABLE</code> statement.
 * 
 * @author Matthew T. Adams
 */
public class AlterColumnCqlGenerator extends ColumnChangeCqlGenerator<AlterColumnSpecification> {

	public AlterColumnCqlGenerator(AlterColumnSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("ALTER ").append(spec().getNameAsIdentifier()).append(" TYPE ")
				.append(spec().getType().getName());
	}
}
