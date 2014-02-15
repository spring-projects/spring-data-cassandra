/*******************************************************************************
 * Copyright 2013-2014 the original author or authors.
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
 ******************************************************************************/
/*
 * Copyright 2011-2014 the original author or authors.
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

import java.util.List;

import org.springframework.cassandra.core.Ordering;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.util.TypeInformation;

import com.datastax.driver.core.DataType;

/**
 * Cassandra specific {@link org.springframework.data.mapping.PersistentProperty} extension.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David T. Webb
 */
public interface CassandraPersistentProperty extends PersistentProperty<CassandraPersistentProperty> {

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
	String getColumnName();

	/**
	 * The names of the columns to which the property is persisted if this is a composite primary key property. Never
	 * returns null.
	 */
	List<String> getColumnNames();

	/**
	 * The ordering (ascending or descending) for the column. Valid only for primary key columns; returns null for
	 * non-primary key columns.
	 */
	Ordering getPrimaryKeyOrdering();

	/**
	 * The column's data type. Not valid for a composite primary key, in which case this method returns null.
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
}
