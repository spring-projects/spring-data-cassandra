package org.springframework.cassandra.core.cql.generator;

import org.springframework.cassandra.core.keyspace.TableNameSpecification;
import org.springframework.util.Assert;

public abstract class TableNameCqlGenerator<T extends TableNameSpecification<T>> {

	public abstract StringBuilder toCql(StringBuilder cql);

	private TableNameSpecification<T> specification;

	public TableNameCqlGenerator(TableNameSpecification<T> specification) {
		setSpecification(specification);
	}

	protected void setSpecification(TableNameSpecification<T> specification) {
		Assert.notNull(specification);
		this.specification = specification;
	}

	@SuppressWarnings("unchecked")
	public T getSpecification() {
		return (T) specification;
	}

	/**
	 * Convenient synonymous method of {@link #getSpecification()}.
	 */
	protected T spec() {
		return getSpecification();
	}

	public String toCql() {
		return toCql(null).toString();
	}
}
