package org.springframework.cassandra.core.keyspace;

public class CqlBuilder {

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to create a table. Convenient if imported statically.
	 */
	public static CreateTableSpecification createTable() {
		return new CreateTableSpecification();
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to create a table. Convenient if imported statically.
	 */
	public static CreateTableSpecification createTable(String name) {
		return new CreateTableSpecification().name(name);
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to alter a table. Convenient if imported statically.
	 */
	public static AlterTableSpecification alterTable() {
		return new AlterTableSpecification();
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to alter a table. Convenient if imported statically.
	 */
	public static AlterTableSpecification alterTable(String name) {
		return new AlterTableSpecification().name(name);
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to drop a table. Convenient if imported statically.
	 */
	public static DropTableSpecification dropTable() {
		return new DropTableSpecification();
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to drop a table. Convenient if imported statically.
	 */
	public static DropTableSpecification dropTable(String name) {
		return new DropTableSpecification().name(name);
	}
}