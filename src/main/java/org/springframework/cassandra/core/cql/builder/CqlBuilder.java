package org.springframework.cassandra.core.cql.builder;

public class CqlBuilder {

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to create a table. Convenient if imported statically.
	 */
	public static CreateTableBuilder createTable() {
		return new CreateTableBuilder();
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to create a table. Convenient if imported statically.
	 */
	public static CreateTableBuilder createTable(String name) {
		return new CreateTableBuilder().name(name);
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to alter a table. Convenient if imported statically.
	 */
	public static AlterTableBuilder alterTable() {
		return new AlterTableBuilder();
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to alter a table. Convenient if imported statically.
	 */
	public static AlterTableBuilder alterTable(String name) {
		return new AlterTableBuilder().name(name);
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to drop a table. Convenient if imported statically.
	 */
	public static DropTableBuilder dropTable() {
		return new DropTableBuilder();
	}

	/**
	 * Entry point into the {@link CqlBuilder}'s fluent API to drop a table. Convenient if imported statically.
	 */
	public static DropTableBuilder dropTable(String name) {
		return new DropTableBuilder().name(name);
	}
}