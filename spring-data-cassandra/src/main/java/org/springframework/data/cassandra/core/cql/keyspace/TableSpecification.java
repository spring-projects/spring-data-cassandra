/*
 * Copyright 2013-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.keyspace;

import static org.springframework.data.cassandra.core.cql.PrimaryKeyType.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Object to support the configuration of table specifications that have columns. This class can also be used as a
 * standalone {@link TableDescriptor}, independent of {@link CreateTableSpecification}.
 *
 * @author Matthew T. Adams
 * @author Alex Shvid
 * @author Mark Paluch
 * @author Aleksei Zotov
 */
public class TableSpecification<T> extends TableOptionsSpecification<TableSpecification<T>> implements TableDescriptor {

	/**
	 * List of all columns.
	 */
	private final List<ColumnSpecification> columns = new ArrayList<>();

	/**
	 * List of only those columns that comprise the partition key.
	 */
	private final List<ColumnSpecification> partitionKeyColumns = new ArrayList<>();

	/**
	 * List of only those columns that comprise the primary key that are not also part of the partition key.
	 */
	private final List<ColumnSpecification> clusteredKeyColumns = new ArrayList<>();

	/**
	 * List of only those columns that are not partition or primary key columns.
	 */
	private final List<ColumnSpecification> nonKeyColumns = new ArrayList<>();

	/**
	 * List of static columns.
	 */
	private final List<ColumnSpecification> staticColumns = new ArrayList<>();

	protected TableSpecification(CqlIdentifier name) {
		super(name);
	}

	protected TableSpecification(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {
		super(keyspace, name);
	}

	/**
	 * Adds the given non-key column to the table. Must be specified after all primary key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 */
	public T column(String name, DataType type) {
		return column(CqlIdentifier.fromCql(name), type);
	}

	/**
	 * Adds the given non-key column to the table. Must be specified after all primary key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 */
	public T column(CqlIdentifier name, DataType type) {
		return column(name, type, Optional.empty(), Optional.empty(), false);
	}

	/**
	 * Adds the given static column to the table. Must be specified after all primary key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @since 3.2
	 */
	public T staticColumn(String name, DataType type) {
		return staticColumn(CqlIdentifier.fromCql(name), type);
	}

	/**
	 * Adds the given static column to the table. Must be specified after all primary key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @since 3.2
	 */
	public T staticColumn(CqlIdentifier name, DataType type) {
		return column(name, type, Optional.empty(), Optional.empty(), true);
	}

	/**
	 * Adds the given partition key column to the table. Must be specified before any other columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @return this
	 */
	public T partitionKeyColumn(String name, DataType type) {
		return partitionKeyColumn(CqlIdentifier.fromCql(name), type);
	}

	/**
	 * Adds the given partition key column to the table. Must be specified before any other columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @return this
	 */
	public T partitionKeyColumn(CqlIdentifier name, DataType type) {
		return column(name, type, Optional.of(PARTITIONED), Optional.empty(), false);
	}

	/**
	 * Adds the given primary key column to the table with ascending ordering. Must be specified after all partition key
	 * columns and before any non-key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @return this
	 */
	public T clusteredKeyColumn(String name, DataType type) {
		return clusteredKeyColumn(CqlIdentifier.fromCql(name), type);
	}

	/**
	 * Adds the given primary key column to the table with ascending ordering. Must be specified after all partition key
	 * columns and before any non-key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @param ordering The data type of the column, must not be {@literal null}.
	 * @return this
	 * @since 2.1
	 */
	public T clusteredKeyColumn(String name, DataType type, Ordering ordering) {

		Assert.notNull(ordering, "Ordering must not be null");

		return column(CqlIdentifier.fromCql(name), type, Optional.of(CLUSTERED), Optional.of(ordering), false);
	}

	/**
	 * Adds the given primary key column to the table with ascending ordering. Must be specified after all partition key
	 * columns and before any non-key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @return this
	 */
	public T clusteredKeyColumn(CqlIdentifier name, DataType type) {
		return clusteredKeyColumn(name, type, Optional.empty());
	}

	/**
	 * Adds the given primary key column to the table with ascending ordering. Must be specified after all partition key
	 * columns and before any non-key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @param ordering The data type of the column, must not be {@literal null}.
	 * @return this
	 */
	public T clusteredKeyColumn(CqlIdentifier name, DataType type, Ordering ordering) {

		Assert.notNull(ordering, "Ordering must not be null");

		return column(name, type, Optional.of(CLUSTERED), Optional.of(ordering), false);
	}

	/**
	 * Adds the given primary key column to the table with ascending ordering. Must be specified after all partition key
	 * columns and before any non-key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @param ordering The data type of the column, must not be {@literal null}.
	 * @return this
	 */
	public T clusteredKeyColumn(CqlIdentifier name, DataType type, Optional<Ordering> ordering) {
		return column(name, type, Optional.of(CLUSTERED), ordering, false);
	}

	/**
	 * Adds the given info as a new column to the table. Partition key columns must precede primary key columns, which
	 * must precede non-key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @param keyType Indicates key type. Null means that the column is not a key column, must not be {@literal null}.
	 * @return this
	 */
	protected T column(String name, DataType type, PrimaryKeyType keyType) {
		return column(name, type, keyType, Optional.empty());
	}

	/**
	 * Adds the given info as a new column to the table. Partition key columns must precede primary key columns, which
	 * must precede non-key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @param keyType Indicates key type, must not be {@literal null}.
	 * @param ordering If the given {@link PrimaryKeyType} is {@link PrimaryKeyType#CLUSTERED}, then the given ordering is
	 *          used, else ignored, must not be {@literal null}.
	 * @return this
	 */
	protected T column(String name, DataType type, PrimaryKeyType keyType, Ordering ordering) {

		Assert.notNull(keyType, "PrimaryKeyType must not be null");
		Assert.notNull(ordering, "Ordering must not be null");

		return column(CqlIdentifier.fromCql(name), type, Optional.of(keyType), Optional.of(ordering), false);
	}

	/**
	 * Adds the given info as a new column to the table. Partition key columns must precede primary key columns, which
	 * must precede non-key columns.
	 *
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes,
	 *          must not be {@literal null}.
	 * @param type The data type of the column, must not be {@literal null}.
	 * @param keyType Indicates key type, must not be {@literal null}.
	 * @param ordering If the given {@link PrimaryKeyType} is {@link PrimaryKeyType#CLUSTERED}, then the given ordering is
	 *          used, else ignored, must not be {@literal null}.
	 * @return this
	 */
	protected T column(String name, DataType type, PrimaryKeyType keyType, Optional<Ordering> ordering) {

		Assert.notNull(keyType, "PrimaryKeyType must not be null");
		Assert.notNull(ordering, "Ordering must not be null");

		return column(CqlIdentifier.fromCql(name), type, Optional.of(keyType), ordering, false);
	}

	@SuppressWarnings("unchecked")
	protected T column(CqlIdentifier name, DataType type, Optional<PrimaryKeyType> optionalKeyType,
			Optional<Ordering> optionalOrdering, boolean isStatic) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(type, "DataType must not be null");
		Assert.notNull(optionalKeyType, "PrimaryKeyType must not be null");
		Assert.notNull(optionalOrdering, "Ordering must not be null");
		Assert.isTrue(!(optionalKeyType.isPresent() && isStatic),"PrimaryKey must not be static");

		ColumnSpecification column = ColumnSpecification.name(name).type(type);

		optionalKeyType.ifPresent(keyType -> {

			column.keyType(keyType);
			optionalOrdering.filter(o -> keyType == CLUSTERED).ifPresent(column::ordering);

			if (keyType == PrimaryKeyType.PARTITIONED) {
				this.partitionKeyColumns.add(column);
			}

			if (keyType == PrimaryKeyType.CLUSTERED) {
				this.clusteredKeyColumns.add(column);
			}
		});

		this.columns.add(column);

		if (optionalKeyType.isEmpty()) {
			this.nonKeyColumns.add(column);
		}

		if (isStatic) {
			column.staticColumn();
			this.staticColumns.add(column);
		}

		return (T) this;
	}

	/**
	 * Returns an unmodifiable list of all columns.
	 */
	@Override
	public List<ColumnSpecification> getColumns() {
		return Collections.unmodifiableList(this.columns);
	}

	/**
	 * Returns an unmodifiable list of all partition key columns.
	 */
	@Override
	public List<ColumnSpecification> getPartitionKeyColumns() {
		return Collections.unmodifiableList(this.partitionKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all primary key columns that are not also partition key columns.
	 */
	@Override
	public List<ColumnSpecification> getClusteredKeyColumns() {
		return Collections.unmodifiableList(this.clusteredKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all primary key columns that are not also partition key columns.
	 */
	@Override
	public List<ColumnSpecification> getPrimaryKeyColumns() {

		List<ColumnSpecification> primaryKeyColumns = new ArrayList<>();

		primaryKeyColumns.addAll(this.partitionKeyColumns);
		primaryKeyColumns.addAll(this.clusteredKeyColumns);

		return Collections.unmodifiableList(primaryKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all non-key columns.
	 */
	@Override
	public List<ColumnSpecification> getNonKeyColumns() {
		return Collections.unmodifiableList(this.nonKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of static columns.
	 */
	@Override
	public List<ColumnSpecification> getStaticColumns() {
		return Collections.unmodifiableList(this.staticColumns);
	}
}
