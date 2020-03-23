/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Cassandra specific {@link PersistentEntity} abstraction.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public interface CassandraPersistentEntity<T> extends PersistentEntity<T, CassandraPersistentProperty> {

	/**
	 * Returns whether this entity represents a composite primary key.
	 */
	boolean isCompositePrimaryKey();

	/**
	 * Sets whether to enforce quoting when using the {@link #getTableName()} in CQL.
	 *
	 * @param forceQuote {@literal true} to enforce quoting; {@literal false} to disable enforced quoting usage.
	 * @deprecated since 3.0. The table name gets converted into {@link com.datastax.oss.driver.api.core.CqlIdentifier}
	 *             hence it no longer requires an indication whether the name should be quoted.
	 * @see com.datastax.oss.driver.api.core.CqlIdentifier#fromInternal(String)
	 */
	@Deprecated
	void setForceQuote(boolean forceQuote);

	/**
	 * Returns the table name to which the entity shall be persisted.
	 */
	CqlIdentifier getTableName();

	/**
	 * Sets the CQL table name.
	 *
	 * @param tableName must not be {@literal null}.
	 * @deprecated since 3.0, use {@link #setTableName(CqlIdentifier)} instead.
	 */
	@Deprecated
	default void setTableName(org.springframework.data.cassandra.core.cql.CqlIdentifier tableName) {

		Assert.notNull(tableName, "Table name must not be null");
		setTableName(tableName.toCqlIdentifier());
	}

	/**
	 * Sets the CQL table name.
	 *
	 * @param tableName must not be {@literal null}.
	 */
	void setTableName(CqlIdentifier tableName);

	/**
	 * @return {@literal true} if the type is a mapped tuple type.
	 * @since 2.1
	 * @see Tuple
	 */
	boolean isTupleType();

	/**
	 * @return {@literal true} if the type is a mapped user defined type.
	 * @since 1.5
	 * @see UserDefinedType
	 */
	boolean isUserDefinedType();

}
