/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import java.util.Map;
import java.util.Optional;

import org.springframework.data.cassandra.core.convert.SchemaFactory;
import org.springframework.data.cassandra.core.mapping.Table;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Operations for managing a Cassandra keyspace.
 *
 * @author David Webb
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author Fabio J. Mendes
 */
public interface CassandraAdminOperations extends CassandraOperations {

	/**
	 * @return the {@link SchemaFactory}.
	 * @since 3.0
	 */
	SchemaFactory getSchemaFactory();

	/**
	 * Create a table with the name given and fields corresponding to the given class. If the table already exists and
	 * parameter {@code ifNotExists} is {@literal true}, this is a no-op and {@literal false} is returned. If the table
	 * doesn't exist, parameter {@code ifNotExists} is ignored, the table is created and {@literal true} is returned.
	 *
	 * @param ifNotExists If true, will only create the table if it doesn't exist, else the create operation will be
	 *          ignored.
	 * @param tableName The name of the table.
	 * @param entityClass The class whose fields determine the columns created.
	 * @param optionsByName Table options, given by the string option name and the appropriate option value.
	 */
	void createTable(boolean ifNotExists, CqlIdentifier tableName, Class<?> entityClass,
			Map<String, Object> optionsByName);

	/**
	 * Drops a table based on the given {@link Class entity type}. The name of the table is derived from either the simple
	 * name of the {@link Class entity class} or name of the table specified with the {@link Table} mapping annotation.
	 *
	 * @param entityType {@link Class type} of the entity for which the table will be dropped.
	 */
	void dropTable(Class<?> entityType);

	/**
	 * Drops the {@link String named} table.
	 *
	 * @param tableName {@link String Name} of the table to drop.
	 * @see #dropTable(boolean, CqlIdentifier)
	 */
	void dropTable(CqlIdentifier tableName);

	/**
	 * Drops the {@link String named} table.
	 *
	 * @param ifExists If {@literal true}, will only drop the table if it exists, else the drop operation will be ignored.
	 * @param tableName {@link String Name} of the table to drop.
	 * @since 2.1
	 */
	void dropTable(boolean ifExists, CqlIdentifier tableName);

	/**
	 * Drops a user type.
	 *
	 * @param typeName must not be {@literal null}.
	 * @since 1.5
	 */
	void dropUserType(CqlIdentifier typeName);

	/**
	 * Returns {@link KeyspaceMetadata} for the current keyspace.
	 *
	 * @return {@link KeyspaceMetadata} for the current keyspace.
	 * @since 1.5
	 */
	KeyspaceMetadata getKeyspaceMetadata();

	/**
	 * Lookup {@link TableMetadata}.
	 *
	 * @param keyspace must not be empty or {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the {@link TableMetadata} or {@literal null}.
	 */
	default Optional<TableMetadata> getTableMetadata(String keyspace, CqlIdentifier tableName) {
		return getTableMetadata(CqlIdentifier.fromCql(keyspace), tableName);
	}

	/**
	 * Lookup {@link TableMetadata}.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the {@link TableMetadata} or {@literal null}.
	 * @since 3.0
	 */
	Optional<TableMetadata> getTableMetadata(CqlIdentifier keyspace, CqlIdentifier tableName);
}
