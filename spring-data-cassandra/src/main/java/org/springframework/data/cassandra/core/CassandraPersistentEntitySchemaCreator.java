/*
 * Copyright 2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateUserTypeCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;

/**
 * Schema creation support for Cassandra based on {@link CassandraMappingContext} and {@link CassandraPersistentEntity}.
 * This class generates CQL to drop, recreate and create user types (UDT) and tables.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see org.springframework.data.cassandra.mapping.Table
 * @see org.springframework.data.cassandra.mapping.UserDefinedType
 * @see org.springframework.data.cassandra.mapping.CassandraType
 */
public class CassandraPersistentEntitySchemaCreator {

	private final CassandraMappingContext mappingContext;
	private final CassandraAdminOperations cassandraAdminOperations;

	/**
	 * Creates a new {@link CassandraPersistentEntitySchemaCreator} for the given {@link CassandraMappingContext} and
	 * {@link CassandraAdminOperations}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param cassandraAdminOperations must not be {@literal null}.
	 */
	public CassandraPersistentEntitySchemaCreator(CassandraMappingContext mappingContext,
			CassandraAdminOperations cassandraAdminOperations) {

		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");
		Assert.notNull(cassandraAdminOperations, "CassandraAdminOperations must not be null");

		this.mappingContext = mappingContext;
		this.cassandraAdminOperations = cassandraAdminOperations;
	}

	/**
	 * Create user types. Can drop types and drop unused types.
	 *
	 * @param dropUserTypes {@literal true} to drop types before creation.
	 * @param dropUnused {@literal true} to drop unused types before creation. Type usage is determined from existing
	 *          mapped {@link org.springframework.data.cassandra.mapping.UserDefinedType}s and UDT names on field
	 *          specifications.
	 * @param ifNotExists {@literal true} to create types using {@code IF NOT EXISTS}.
	 */
	public void createUserTypes(boolean dropUserTypes, boolean dropUnused, boolean ifNotExists) {

		if (dropUserTypes) {
			dropUserTypes(dropUnused);
		}

		List<CreateUserTypeSpecification> specifications = createUserTypeSpecifications(ifNotExists);

		for (CreateUserTypeSpecification specification : specifications) {
			cassandraAdminOperations.getCqlOperations().execute(CreateUserTypeCqlGenerator.toCql(specification));
		}
	}

	/**
	 * Create user types. Can drop types and drop unused types.
	 *
	 * @param dropTables {@literal true} to drop tables before creation.
	 * @param dropUnused {@literal true} to drop unused tables before creation. Table usage is determined by existing
	 *          table mappings.
	 * @param ifNotExists {@literal true} to create tables using {@code IF NOT EXISTS}.
	 */
	public void createTables(boolean dropTables, boolean dropUnused, boolean ifNotExists) {

		if (dropTables) {
			dropTables(dropUnused);
		}

		List<CreateTableSpecification> specifications = createTableSpecifications(ifNotExists);

		for (CreateTableSpecification specification : specifications) {
			cassandraAdminOperations.getCqlOperations().execute(CreateTableCqlGenerator.toCql(specification));
		}
	}

	protected List<CreateUserTypeSpecification> createUserTypeSpecifications(boolean ifNotExists) {

		Collection<? extends CassandraPersistentEntity<?>> entities = new ArrayList<CassandraPersistentEntity<?>>(
				mappingContext.getUserDefinedTypeEntities());

		Map<CqlIdentifier, CassandraPersistentEntity<?>> byName = new HashMap<CqlIdentifier, CassandraPersistentEntity<?>>();

		for (CassandraPersistentEntity<?> entity : entities) {
			byName.put(entity.getTableName(), entity);
		}

		List<CreateUserTypeSpecification> specifications = new ArrayList<CreateUserTypeSpecification>();

		Set<CqlIdentifier> created = new HashSet<CqlIdentifier>();

		for (CassandraPersistentEntity<?> entity : entities) {

			Set<CqlIdentifier> seen = new LinkedHashSet<CqlIdentifier>();
			seen.add(entity.getTableName());
			visitUserTypes(entity, seen);

			List<CqlIdentifier> ordered = new ArrayList<CqlIdentifier>(seen);
			Collections.reverse(ordered);

			for (CqlIdentifier identifier : ordered) {

				if (created.add(identifier)) {
					specifications.add(mappingContext.getCreateUserTypeSpecificationFor(
						byName.get(identifier)).ifNotExists(ifNotExists));
				}
			}
		}
		return specifications;
	}

	protected List<CreateTableSpecification> createTableSpecifications(boolean ifNotExists) {
		Collection<? extends CassandraPersistentEntity<?>> entities = new ArrayList<CassandraPersistentEntity<?>>(
				mappingContext.getNonPrimaryKeyEntities());

		List<CreateTableSpecification> specifications = new ArrayList<CreateTableSpecification>();

		for (CassandraPersistentEntity<?> entity : entities) {
			specifications.add(mappingContext.getCreateTableSpecificationFor(entity).ifNotExists(ifNotExists));
		}

		return specifications;
	}

	private void visitUserTypes(CassandraPersistentEntity<?> entity, final Set<CqlIdentifier> seen) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty persistentProperty) {

				CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(persistentProperty);
				if (persistentEntity != null && persistentEntity.isUserDefinedType()) {
					if (seen.add(persistentEntity.getTableName())) {
						visitUserTypes(persistentEntity, seen);
					}
				}
			}
		});
	}

	private void dropUserTypes(boolean dropUnused) {

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

	private void dropTables(boolean dropUnused) {

		KeyspaceMetadata keyspaceMetadata = cassandraAdminOperations.getKeyspaceMetadata();

		for (TableMetadata table : keyspaceMetadata.getTables()) {
			if (dropUnused || mappingContext.usesTable(table)) {
				cassandraAdminOperations.dropTable(CqlIdentifier.cqlId(table.getName()));
			}
		}
	}
}
