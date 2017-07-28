/*
 * Copyright 2013-2017 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.Map;
import java.util.Optional;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

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
	 * Create a table with the name given and fields corresponding to the given class. If the table already exists and
	 * parameter {@code ifNotExists} is {@literal true}, this is a no-op and {@literal false} is returned. If the table
	 * doesn't exist, parameter {@code ifNotExists} is ignored, the table is created and {@literal true} is returned.
	 *
	 * @param ifNotExists If true, will only create the table if it doesn't exist, else the create operation will be
	 *          ignored and the method will return {@literal false}.
	 * @param tableName The name of the table.
	 * @param entityClass The class whose fields determine the columns created.
	 * @param optionsByName Table options, given by the string option name and the appropriate option value.
	 */
	void createTable(boolean ifNotExists, CqlIdentifier tableName, Class<?> entityClass,
			Map<String, Object> optionsByName);

	/**
	 * Drops the named table.
	 *
	 * @param tableName The name of the table.
	 */
	void dropTable(CqlIdentifier tableName);

	/**
	 * Lookup {@link TableMetadata}.
	 *
	 * @param keyspace must not be empty or {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the {@link TableMetadata} or {@literal null}.
	 */
	Optional<TableMetadata> getTableMetadata(String keyspace, CqlIdentifier tableName);

	/**
	 * Returns {@link KeyspaceMetadata} for the current keyspace.
	 *
	 * @return {@link KeyspaceMetadata} for the current keyspace.
	 * @since 1.5
	 */
	KeyspaceMetadata getKeyspaceMetadata();

	/**
	 * Drops a user type.
	 *
	 * @param typeName must not be {@literal null}.
	 * @since 1.5
	 */
	void dropUserType(CqlIdentifier typeName);
}
