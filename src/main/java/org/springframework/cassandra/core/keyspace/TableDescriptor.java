package org.springframework.cassandra.core.keyspace;

import java.util.List;
import java.util.Map;

/**
 * Describes a table.
 * 
 * @author Matthew T. Adams
 */
public interface TableDescriptor {

	/**
	 * Returns the name of the table.
	 */
	String getName();

	/**
	 * Returns the name of the table as an identifer or quoted identifier as appropriate.
	 */
	String getNameAsIdentifier();

	/**
	 * Returns an unmodifiable {@link List} of {@link ColumnSpecification}s.
	 */
	List<ColumnSpecification> getColumns();

	/**
	 * Returns an unmodifiable list of all partition key columns.
	 */
	public List<ColumnSpecification> getPartitionKeyColumns();

	/**
	 * Returns an unmodifiable list of all primary key columns that are not also partition key columns.
	 */
	public List<ColumnSpecification> getPrimaryKeyColumns();

	/**
	 * Returns an unmodifiable list of all partition and primary key columns.
	 */
	public List<ColumnSpecification> getKeyColumns();

	/**
	 * Returns an unmodifiable list of all non-key columns.
	 */
	public List<ColumnSpecification> getNonKeyColumns();

	/**
	 * Returns an unmodifiable {@link Map} of table options.
	 */
	Map<String, Object> getOptions();
}
