package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.DropColumnSpecification;

/**
 * CQL generator for generating a <code>DROP</code> column clause of an <code>ALTER TABLE</code> statement.
 * 
 * @author Matthew T. Adams
 */
public class DropColumnCqlGenerator extends ColumnChangeCqlGenerator<DropColumnSpecification> {

	public DropColumnCqlGenerator(DropColumnSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("DROP ").append(spec().getNameAsIdentifier());
	}
}
