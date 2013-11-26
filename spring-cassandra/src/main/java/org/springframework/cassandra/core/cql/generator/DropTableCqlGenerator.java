package org.springframework.cassandra.core.cql.generator;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.DropTableSpecification;

/**
 * CQL generator for generating a <code>DROP TABLE</code> statement.
 * 
 * @author Matthew T. Adams
 */
public class DropTableCqlGenerator extends TableNameCqlGenerator<DropTableSpecification> {

	public DropTableCqlGenerator(DropTableSpecification specification) {
		super(specification);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("DROP TABLE ").append(spec().getIfExists() ? "IF EXISTS " : "")
				.append(spec().getNameAsIdentifier()).append(";");
	}
}
