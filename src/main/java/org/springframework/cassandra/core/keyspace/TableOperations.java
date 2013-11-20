package org.springframework.cassandra.core.keyspace;

/**
 * Class that offers static methods as entry points into the fluent API for building create, drop and alter table
 * specifications. These methods are most convenient when imported statically.
 * 
 * @author Matthew T. Adams
 */
public class TableOperations {

	/**
	 * Entry point into the {@link CreateTableSpecification}'s fluent API to create a table. Convenient if imported
	 * statically.
	 */
	public static CreateTableSpecification createTable() {
		return new CreateTableSpecification();
	}

	/**
	 * Entry point into the {@link DropTableSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically.
	 */
	public static DropTableSpecification dropTable() {
		return new DropTableSpecification();
	}

	/**
	 * Entry point into the {@link AlterTableSpecification}'s fluent API to alter a table. Convenient if imported
	 * statically.
	 */
	public static AlterTableSpecification alterTable() {
		return new AlterTableSpecification();
	}
}