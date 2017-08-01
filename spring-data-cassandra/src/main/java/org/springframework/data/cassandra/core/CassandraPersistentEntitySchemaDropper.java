/*
 * Copyright 2017 the original author or authors.
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

import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.AbstractTableMetadata;

/**
 * Schema drop support for Cassandra based on {@link CassandraMappingContext} and {@link CassandraPersistentEntity}.
 * This class generates CQL to drop user types (UDT) and tables.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see org.springframework.data.cassandra.core.mapping.Table
 * @see org.springframework.data.cassandra.core.mapping.UserDefinedType
 * @see org.springframework.data.cassandra.core.mapping.CassandraType
 */
public class CassandraPersistentEntitySchemaDropper {

	private final CassandraAdminOperations cassandraAdminOperations;

	private final CassandraMappingContext mappingContext;

	/**
	 * Create a new {@link CassandraPersistentEntitySchemaDropper} for the given {@link CassandraMappingContext} and
	 * {@link CassandraAdminOperations}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param cassandraAdminOperations must not be {@literal null}.
	 */
	public CassandraPersistentEntitySchemaDropper(CassandraMappingContext mappingContext,
			CassandraAdminOperations cassandraAdminOperations) {

		Assert.notNull(cassandraAdminOperations, "CassandraAdminOperations must not be null");
		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");

		this.cassandraAdminOperations = cassandraAdminOperations;
		this.mappingContext = mappingContext;
	}

	/**
	 * Drop tables that exist in the keyspace.
	 *
	 * @param dropUnused {@literal true} to drop unused tables. Table usage is determined by existing table mappings.
	 */
	public void dropTables(boolean dropUnused) {

		this.cassandraAdminOperations.getKeyspaceMetadata().getTables()
				.stream()
				.map(AbstractTableMetadata::getName)
				.map(CqlIdentifier::of)
				.filter(table -> dropUnused || this.mappingContext.usesTable(table))
				.forEach(this.cassandraAdminOperations::dropTable);
	}

	/**
	 * Drop user types that exist in the keyspace.
	 *
	 * @param dropUnused {@literal true} to drop unused types before creation. Type usage is determined from existing
	 *          mapped {@link org.springframework.data.cassandra.core.mapping.UserDefinedType}s and UDT names on field
	 *          specifications.
	 */
	public void dropUserTypes(boolean dropUnused) {

		Set<CqlIdentifier> canRecreate = this.mappingContext.getUserDefinedTypeEntities().stream()
				.map(CassandraPersistentEntity::getTableName).collect(Collectors.toSet());

		this.cassandraAdminOperations.getKeyspaceMetadata().getUserTypes().forEach(userType -> {

			CqlIdentifier typeName = CqlIdentifier.of(userType.getTypeName());

			if (canRecreate.contains(typeName)) {
				this.cassandraAdminOperations.dropUserType(typeName);
			} else if (dropUnused && !mappingContext.usesUserType(typeName)) {
				this.cassandraAdminOperations.dropUserType(typeName);
			}
		});
	}
}
