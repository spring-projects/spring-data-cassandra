package org.springframework.data.cassandra.mapping;

/**
 * Enum for Cassandra primary key column ordering.
 * 
 * @author Matthew T. Adams
 */
public enum Ordering {

	/**
	 * Ascending Cassandra column ordering.
	 */
	ASCENDING("ASC"),

	/**
	 * Descending Cassandra column ordering.
	 */
	DESCENDING("DESC");

	private String cql;

	private Ordering(String cql) {
		this.cql = cql;
	}

	/**
	 * Returns the CQL keyword of this {@link Ordering}.
	 */
	public String cql() {
		return cql;
	}
}