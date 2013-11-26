package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.cassandra.core.cql.CqlStringUtils.identifize;

/**
 * Abstract builder class to support the construction of table specifications.
 * 
 * @author Matthew T. Adams
 * @param <T> The subtype of the {@link TableNameSpecification}
 */
public abstract class TableNameSpecification<T extends TableNameSpecification<T>> {

	/**
	 * The name of the table.
	 */
	private String name;

	/**
	 * Sets the table name.
	 * 
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public T name(String name) {
		checkIdentifier(name);
		this.name = name;
		return (T) this;
	}

	public String getName() {
		return name;
	}

	public String getNameAsIdentifier() {
		return identifize(name);
	}
}
