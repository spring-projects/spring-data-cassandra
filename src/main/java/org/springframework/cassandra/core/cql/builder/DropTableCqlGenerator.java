package org.springframework.cassandra.core.cql.builder;

import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;

import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.util.Assert;

public class DropTableCqlGenerator {

	protected DropTableSpecification specification;

	public DropTableCqlGenerator(DropTableSpecification specification) {
		setSpecification(specification);
	}

	protected void setSpecification(DropTableSpecification specification) {
		Assert.notNull(specification);
		this.specification = specification;
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("DROP TABLE ").append(specification.getIfExists() ? "IF EXISTS " : "")
				.append(specification.getNameAsIdentifier());
	}

	public String toCql() {
		return toCql(null).toString();
	}
}
