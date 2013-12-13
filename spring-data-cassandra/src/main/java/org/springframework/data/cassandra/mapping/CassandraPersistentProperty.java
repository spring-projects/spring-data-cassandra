/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import org.springframework.cassandra.core.Ordering;
import org.springframework.data.mapping.PersistentProperty;

import com.datastax.driver.core.DataType;

/**
 * Cassandra specific {@link org.springframework.data.mapping.PersistentProperty} extension.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public interface CassandraPersistentProperty extends PersistentProperty<CassandraPersistentProperty> {

	/**
	 * Whether the property is a composite primary key.
	 */
	boolean isCompositePrimaryKey();

	/**
	 * Returns the type of the composite primary key class of this entity, or null if this class does not use a composite
	 * primary key.
	 */
	Class<?> getCompositePrimaryKeyType();

	/**
	 * Returns a {@link CassandraPersistentEntity} representing the composite primary key class of this entity, or null if
	 * this class does not use a composite primary key.
	 */
	CassandraPersistentEntity<?> getCompositePrimaryKeyEntity();

	/**
	 * The name of the column to which a property is persisted.
	 */
	String getColumnName();

	/**
	 * The ordering for the column. Valid only for clustered columns.
	 */
	Ordering getOrdering();

	/**
	 * The column's data type.
	 */
	DataType getDataType();

	/**
	 * Whether the property has secondary index on this column.
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
}
