/*
 * Copyright 2013-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentEntity;

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
	 * Retrieve a {@link CassandraPersistentProperty} from a {@link Parameter persistence creator (constructor/factory
	 * method) parameter}. Parameters are either derived by name or synthesized if their name does not map to a existing
	 * property.
	 *
	 * @param parameter the parameter to create a property from. Parameters without a name return no ({@literal null})
	 *          parameter.
	 * @return the property, synthetic property or {@literal null}, if the parameter is unnamed.
	 * @since 4.2.3
	 */
	@Nullable
	CassandraPersistentProperty getProperty(Parameter<?, CassandraPersistentProperty> parameter);

	/**
	 * Returns whether this entity represents a composite primary key.
	 */
	boolean isCompositePrimaryKey();

	/**
	 * Returns the table name to which the entity shall be persisted.
	 */
	CqlIdentifier getTableName();

	/**
	 * Sets the CQL table name.
	 *
	 * @param tableName must not be {@literal null}.
	 */
	void setTableName(CqlIdentifier tableName);

	/**
	 * Returns a specific keyspace to which the entity shall be persisted. If the entity isn't associated with a specific
	 * keyspace, then the entity is persisted in the keyspace associated with the Cassandra session.
	 *
	 * @since 4.4
	 */
	@Nullable
	CqlIdentifier getKeyspace();

	/**
	 * Returns the required keyspace name or throws {@link IllegalStateException} if the entity isn't associated with a
	 * specific keyspace.
	 *
	 * @return the required keyspace name.
	 * @throws IllegalStateException if the entity isn't associated with a specific keyspace.
	 * @since 4.4
	 */
	default CqlIdentifier getRequiredKeyspace() {

		CqlIdentifier keyspace = getKeyspace();

		if (keyspace == null) {
			throw new IllegalStateException(String.format("No keyspace specified for %s", this));
		}

		return keyspace;
	}

	/**
	 * Returns {@code true} if the entity is associated with a specific keyspace; {@code false} otherwise.
	 *
	 * @return {@code true} if the entity is associated with a specific keyspace; {@code false} otherwise.
	 * @since 4.4
	 */
	default boolean hasKeyspace() {
		return getKeyspace() != null;
	}

	/**
	 * Sets the CQL keyspace.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @since 4.4
	 */
	void setKeyspace(CqlIdentifier keyspace);

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
