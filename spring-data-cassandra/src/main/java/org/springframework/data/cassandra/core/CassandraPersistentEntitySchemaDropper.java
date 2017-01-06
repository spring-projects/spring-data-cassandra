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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;

/**
 * Schema drop support for Cassandra based on {@link CassandraMappingContext} and {@link CassandraPersistentEntity}.
 * This class generates CQL to drop user types (UDT) and tables.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see org.springframework.data.cassandra.mapping.Table
 * @see org.springframework.data.cassandra.mapping.UserDefinedType
 * @see org.springframework.data.cassandra.mapping.CassandraType
 */
public class CassandraPersistentEntitySchemaDropper {

	private final CassandraAdminOperations cassandraAdminOperations;
	private final CassandraMappingContext mappingContext;

	/**
	 * Creates a new {@link CassandraPersistentEntitySchemaDropper} for the given {@link CassandraMappingContext} and
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

		KeyspaceMetadata keyspaceMetadata = cassandraAdminOperations.getKeyspaceMetadata();

		for (TableMetadata table : keyspaceMetadata.getTables()) {
			if (dropUnused || mappingContext.usesTable(table)) {
				cassandraAdminOperations.dropTable(CqlIdentifier.cqlId(table.getName()));
			}
		}
	}

	/**
	 * Drop user types that exist in the keyspace.
	 *
	 * @param dropUnused {@literal true} to drop unused types before creation. Type usage is determined from existing
	 *          mapped {@link org.springframework.data.cassandra.mapping.UserDefinedType}s and UDT names on field
	 *          specifications.
	 */
	public void dropUserTypes(boolean dropUnused) {

		KeyspaceMetadata keyspaceMetadata = cassandraAdminOperations.getKeyspaceMetadata();

		Collection<CassandraPersistentEntity<?>> userDefinedTypeEntities = mappingContext.getUserDefinedTypeEntities();
		Set<CqlIdentifier> canRecreate = new HashSet<CqlIdentifier>();

		for (CassandraPersistentEntity<?> userDefinedTypeEntity : userDefinedTypeEntities) {
			canRecreate.add(userDefinedTypeEntity.getTableName());
		}

		for (UserType userType : keyspaceMetadata.getUserTypes()) {
			CqlIdentifier identifier = CqlIdentifier.cqlId(userType.getTypeName());

			if (canRecreate.contains(identifier)) {
				cassandraAdminOperations.dropUserType(identifier);
			} else if (dropUnused && !mappingContext.usesUserType(userType)) {
				cassandraAdminOperations.dropUserType(identifier);
			}
		}
	}
}
