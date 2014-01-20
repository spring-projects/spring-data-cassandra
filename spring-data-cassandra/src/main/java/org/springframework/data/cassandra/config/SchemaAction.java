package org.springframework.data.cassandra.config;

/**
 * Enum identifying any schema actions to take at startup.
 * 
 * @author Matthew T. Adams
 */
public enum SchemaAction {

	/**
	 * Take no schema actions.
	 */
	NONE,

	/**
	 * Create each table as necessary. Fail if a table already exists.
	 */
	CREATE,

	/**
	 * Create each table as necessary, dropping the table first if it exists.
	 */
	RECREATE,

	/**
	 * Drop <em>all</em> tables in the keyspace, then create each table as necessary.
	 */
	RECREATE_DROP_UNUSED;

	// TODO:
	// /**
	// * Validate that each required table and column exists. Fail if any required table or column does not exists.
	// */
	// VALIDATE("VALIDATE"),
	//
	// /**
	// * Alter or create each table and column as necessary, leaving unused tables and columns untouched.
	// */
	// UPDATE("UPDATE"),
	//
	// /**
	// * Alter or create each table and column as necessary, removing unused tables and columns.
	// */
	// UPDATE_DROP_UNUNSED("UPDATE_DROP_UNUSED");
}
