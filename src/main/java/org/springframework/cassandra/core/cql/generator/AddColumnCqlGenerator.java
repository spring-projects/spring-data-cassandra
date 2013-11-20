package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.AddColumnSpecification;

/**
 * CQL generator for generating an <code>ADD</code> clause of an <code>ALTER TABLE</code> statement.
 * 
 * @author Matthew T. Adams
 */
public class AddColumnCqlGenerator extends ColumnChangeCqlGenerator<AddColumnSpecification> {

	public AddColumnCqlGenerator(AddColumnSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("ADD ").append(spec().getNameAsIdentifier()).append(" TYPE ")
				.append(spec().getType().getName());
	}
}
