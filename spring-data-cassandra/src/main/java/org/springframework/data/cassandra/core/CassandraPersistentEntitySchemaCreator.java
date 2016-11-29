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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

	private final CassandraAdminOperations cassandraAdminOperations;
	private final CassandraMappingContext mappingContext;

	/**
	 * Creates a new {@link CassandraPersistentEntitySchemaCreator} for the given {@link CassandraMappingContext} and
	 * {@link CassandraAdminOperations}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param cassandraAdminOperations must not be {@literal null}.
	 */
	public CassandraPersistentEntitySchemaCreator(CassandraMappingContext mappingContext,
			CassandraAdminOperations cassandraAdminOperations) {

		Assert.notNull(cassandraAdminOperations, "CassandraAdminOperations must not be null");
		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");

		this.cassandraAdminOperations = cassandraAdminOperations;
		this.mappingContext = mappingContext;
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

		createTableSpecifications(ifNotExists).forEach(specification ->
			cassandraAdminOperations.getCqlOperations().execute(CreateTableCqlGenerator.toCql(specification)));
	}

	/* (non-Javadoc) */
	protected List<CreateTableSpecification> createTableSpecifications(boolean ifNotExists) {
		return mappingContext.getTableEntities().stream()
			.map(entity -> mappingContext.getCreateTableSpecificationFor(entity).ifNotExists(ifNotExists))
			.collect(Collectors.toList());
	}

	/* (non-Javadoc) */
	private void dropTables(boolean dropUnused) {
		cassandraAdminOperations.getKeyspaceMetadata().getTables().stream()
			.filter(table -> dropUnused || mappingContext.usesTable(table))
			.forEach(table -> cassandraAdminOperations.dropTable(CqlIdentifier.cqlId(table.getName())));
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

		createUserTypeSpecifications(ifNotExists).forEach(specification ->
			cassandraAdminOperations.getCqlOperations().execute(CreateUserTypeCqlGenerator.toCql(specification)));
	}

	/* (non-Javadoc) */
	protected List<CreateUserTypeSpecification> createUserTypeSpecifications(boolean ifNotExists) {

		Collection<? extends CassandraPersistentEntity<?>> entities = new ArrayList<>(
				mappingContext.getUserDefinedTypeEntities());

		Map<CqlIdentifier, CassandraPersistentEntity<?>> byTableName = entities.stream().collect(Collectors.toMap(
			CassandraPersistentEntity::getTableName, entity -> entity));

		List<CreateUserTypeSpecification> specifications = new ArrayList<>();

		// TODO is this Set really needed?
		Set<CqlIdentifier> created = new HashSet<>();

		for (CassandraPersistentEntity<?> entity : entities) {
			Set<CqlIdentifier> seen = new LinkedHashSet<>();

			seen.add(entity.getTableName());
			visitUserTypes(entity, seen);

			List<CqlIdentifier> ordered = new ArrayList<>(seen);
			Collections.reverse(ordered);

			specifications.addAll(ordered.stream().filter(created::add)
				.map(identifier -> mappingContext.getCreateUserTypeSpecificationFor(byTableName.get(identifier))
					.ifNotExists(ifNotExists)).collect(Collectors.toList()));
		}

		return specifications;
	}

	/* (non-Javadoc) */
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

	/* (non-Javadoc) */
	private void dropUserTypes(boolean dropUnused) {

		Set<CqlIdentifier> canRecreate = mappingContext.getUserDefinedTypeEntities().stream()
				.map(CassandraPersistentEntity::getTableName).collect(Collectors.toSet());

		cassandraAdminOperations.getKeyspaceMetadata().getUserTypes().forEach(userType -> {
			CqlIdentifier identifier = CqlIdentifier.cqlId(userType.getTypeName());

			if (canRecreate.contains(identifier)) {
				cassandraAdminOperations.dropUserType(identifier);
			} else if (dropUnused && !mappingContext.usesUserType(userType)) {
				cassandraAdminOperations.dropUserType(identifier);
			}
		});
	}
}
