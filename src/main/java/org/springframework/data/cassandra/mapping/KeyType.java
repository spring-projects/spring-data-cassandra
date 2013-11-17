package org.springframework.data.cassandra.mapping;

/**
 * Values representing primary key column types.
 * 
 * @author Matthew T. Adams
 */
public enum KeyType {

	/**
	 * Used for a column that is a primary key that also is or is part of the partition key.
	 */
	PARTITION,

	/**
	 * Use for a primary key column that is not part of the partition key and, therefore, may also be ordered.
	 */
	PRIMARY
}