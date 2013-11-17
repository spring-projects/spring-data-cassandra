package org.springframework.data.cassandra.cql.builder;

public class CqlBuilder {

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to create a table. Convenient if imported statically.
	 */
	public static CreateTableBuilder createTable() {
		return new CreateTableBuilder();
	}
}
