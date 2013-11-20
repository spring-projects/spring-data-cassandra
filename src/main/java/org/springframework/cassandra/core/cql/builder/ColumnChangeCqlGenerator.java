package org.springframework.cassandra.core.cql.builder;

import org.springframework.cassandra.core.keyspace.ColumnChangeSpecification;
import org.springframework.util.Assert;

public abstract class ColumnChangeCqlGenerator<T extends ColumnChangeSpecification> {

	public abstract StringBuilder toCql(StringBuilder cql);

	private ColumnChangeSpecification specification;

	public ColumnChangeCqlGenerator(ColumnChangeSpecification specification) {
		setSpecification(specification);
	}

	protected void setSpecification(ColumnChangeSpecification specification) {
		Assert.notNull(specification);
		this.specification = specification;
	}

	@SuppressWarnings("unchecked")
	public T getSpecification() {
		return (T) specification;
	}

	protected T spec() {
		return getSpecification();
	}
}
