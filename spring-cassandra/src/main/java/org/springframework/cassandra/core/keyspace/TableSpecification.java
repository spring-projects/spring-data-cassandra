/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.PrimaryKeyType.CLUSTERED;
import static org.springframework.cassandra.core.PrimaryKeyType.PARTITIONED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;

import com.datastax.driver.core.DataType;

/**
 * Builder class to support the construction of table specifications that have columns. This class can also be used as a
 * standalone {@link TableDescriptor}, independent of {@link CreateTableSpecification}.
 * 
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public class TableSpecification<T> extends TableOptionsSpecification<TableSpecification<T>> implements TableDescriptor {

	/**
	 * List of all columns.
	 */
	private List<ColumnSpecification> columns = new ArrayList<ColumnSpecification>();

	/**
	 * List of only those columns that comprise the partition key.
	 */
	private List<ColumnSpecification> partitionKeyColumns = new ArrayList<ColumnSpecification>();

	/**
	 * List of only those columns that comprise the primary key that are not also part of the partition key.
	 */
	private List<ColumnSpecification> clusteredKeyColumns = new ArrayList<ColumnSpecification>();

	/**
	 * List of only those columns that are not partition or primary key columns.
	 */
	private List<ColumnSpecification> nonKeyColumns = new ArrayList<ColumnSpecification>();

	/**
	 * Adds the given non-key column to the table. Must be specified after all primary key columns.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 */
	public T column(String name, DataType type) {
		return column(name, type, null, null);
	}

	/**
	 * Adds the given partition key column to the table. Must be specified before any other columns.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 * @return this
	 */
	public T partitionKeyColumn(String name, DataType type) {
		return column(name, type, PARTITIONED, null);
	}

	/**
	 * Adds the given primary key column to the table with ascending ordering. Must be specified after all partition key
	 * columns and before any non-key columns.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 * @return this
	 */
	public T clusteredKeyColumn(String name, DataType type) {
		return clusteredKeyColumn(name, type, null);
	}

	/**
	 * Adds the given primary key column to the table with the given ordering (<code>null</code> meaning ascending). Must
	 * be specified after all partition key columns and before any non-key columns.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 * @return this
	 */
	public T clusteredKeyColumn(String name, DataType type, Ordering ordering) {
		return column(name, type, CLUSTERED, ordering);
	}

	/**
	 * Adds the given info as a new column to the table. Partition key columns must precede primary key columns, which
	 * must precede non-key columns.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 * @param keyType Indicates key type. Null means that the column is not a key column.
	 * @param ordering If the given {@link PrimaryKeyType} is {@link PrimaryKeyType#CLUSTERED}, then the given ordering is
	 *          used, else ignored.
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	protected T column(String name, DataType type, PrimaryKeyType keyType, Ordering ordering) {

		ColumnSpecification column = new ColumnSpecification().name(name).type(type).keyType(keyType)
				.ordering(keyType == CLUSTERED ? ordering : null);

		columns.add(column);

		if (keyType == PrimaryKeyType.PARTITIONED) {
			partitionKeyColumns.add(column);
		}

		if (keyType == PrimaryKeyType.CLUSTERED) {
			clusteredKeyColumns.add(column);
		}

		if (keyType == null) {
			nonKeyColumns.add(column);
		}

		return (T) this;
	}

	/**
	 * Returns an unmodifiable list of all columns.
	 */
	public List<ColumnSpecification> getColumns() {
		return Collections.unmodifiableList(columns);
	}

	/**
	 * Returns an unmodifiable list of all partition key columns.
	 */
	public List<ColumnSpecification> getPartitionKeyColumns() {
		return Collections.unmodifiableList(partitionKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all primary key columns that are not also partition key columns.
	 */
	public List<ColumnSpecification> getClusteredKeyColumns() {
		return Collections.unmodifiableList(clusteredKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all primary key columns that are not also partition key columns.
	 */
	public List<ColumnSpecification> getPrimaryKeyColumns() {

		ArrayList<ColumnSpecification> primaryKeyColumns = new ArrayList<ColumnSpecification>();
		primaryKeyColumns.addAll(partitionKeyColumns);
		primaryKeyColumns.addAll(clusteredKeyColumns);

		return Collections.unmodifiableList(primaryKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all non-key columns.
	 */
	public List<ColumnSpecification> getNonKeyColumns() {
		return Collections.unmodifiableList(nonKeyColumns);
	}
}
