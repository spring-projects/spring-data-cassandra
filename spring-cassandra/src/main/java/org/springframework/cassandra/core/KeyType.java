package org.springframework.cassandra.core;

/**
 * Values representing primary key column types.
 * 
 * @author Matthew T. Adams
 */
public enum KeyType {

	/**
	 * Used for a column that is a primary key and that also is or is part of the partition key.
	 */
	PARTITION,

	/**
	 * Used for a primary key column that is not part of the partition key.
	 */
	PRIMARY
}