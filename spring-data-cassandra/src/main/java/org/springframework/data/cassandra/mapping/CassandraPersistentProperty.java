/*
 * Copyright 2013-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.mapping;

import java.util.List;
import java.util.Optional;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.util.TypeInformation;

import com.datastax.driver.core.DataType;

/**
 * Cassandra specific {@link org.springframework.data.mapping.PersistentProperty} extension.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David T. Webb
 * @author Mark Paluch
 * @author John Blum
 */
public interface CassandraPersistentProperty
		extends PersistentProperty<CassandraPersistentProperty>, ApplicationContextAware {

	/**
	 * Whether the property is a composite primary key.
	 */
	boolean isCompositePrimaryKey();

	/**
	 * Returns a {@link CassandraPersistentEntity} representing the composite primary key class of this entity, or null if
	 * this class does not use a composite primary key.
	 */
	CassandraPersistentEntity<?> getCompositePrimaryKeyEntity();

	/**
	 * Returns a {@link TypeInformation} representing the type of the composite primary key class of this entity, or null
	 * if this class does not use a composite primary key.
	 */
	TypeInformation<?> getCompositePrimaryKeyTypeInformation();

	/**
	 * Gets the list of composite primary key properties that this composite primary key field is a placeholder for.
	 */
	List<CassandraPersistentProperty> getCompositePrimaryKeyProperties();

	/**
	 * The name of the single column to which the property is persisted. This is a convenience method when the caller
	 * knows that the property is mapped to a single column. Throws {@link IllegalStateException} if this property is
	 * mapped to multiple columns.
	 */
	CqlIdentifier getColumnName();

	/**
	 * The names of the columns to which the property is persisted if this is a composite primary key property. Never
	 * returns null.
	 */
	List<CqlIdentifier> getColumnNames();

	/**
	 * The ordering (ascending or descending) for the column. Valid only for primary key columns; returns null for
	 * non-primary key columns.
	 */
	Optional<Ordering> getPrimaryKeyOrdering();

	/**
	 * The column's data type. Not valid for a composite primary key.
	 *
	 * @return the Cassandra {@link DataType}
	 * @throws InvalidDataAccessApiUsageException if the {@link DataType} cannot be resolved
	 * @see CassandraType
	 */
	DataType getDataType();

	/**
	 * Whether the property has a secondary index on this column.
	 */
	boolean isIndexed();

	/**
	 * Whether the property is a partition key column.
	 */
	boolean isPartitionKeyColumn();

	/**
	 * Whether the property is a cluster key column.
	 */
	boolean isClusterKeyColumn();

	/**
	 * Whether the property is a partition key column or a cluster key column
	 *
	 * @see #isPartitionKeyColumn()
	 * @see #isClusterKeyColumn()
	 */
	boolean isPrimaryKeyColumn();

	@Override
	CassandraPersistentEntity<?> getOwner();

	/**
	 * Whether to force-quote the column names of this property.
	 *
	 * @param forceQuote
	 * @see CassandraPersistentProperty#getColumnNames()
	 */
	void setForceQuote(boolean forceQuote);

	/**
	 * If this property is mapped with a single column, set the column name to the given {@link CqlIdentifier}. If this
	 * property is not mapped by a single column, throws {@link IllegalStateException}. If the given column name is null,
	 * {@link IllegalArgumentException} is thrown.
	 *
	 * @param columnName
	 */
	void setColumnName(CqlIdentifier columnName);

	/**
	 * Sets this property's column names to the collection given. The given collection must have the same size as this
	 * property's current list of column names, and must contain no <code>null</code> elements.
	 *
	 * @param columnName
	 */
	void setColumnNames(List<CqlIdentifier> columnNames);

	/**
	 * Returns whether the property is a {@link java.util.Map}.
	 *
	 * @return a boolean indicating whether this property type is a {@link java.util.Map}.
	 */
	boolean isMapLike();

	enum PropertyToFieldNameConverter implements Converter<CassandraPersistentProperty, String> {

		INSTANCE;

		@Override
		public String convert(CassandraPersistentProperty property) {
			return property.getColumnName().toCql();
		}
	}
}
