/*
 * Copyright 2016-2017 the original author or authors.
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

/**
 * Schema creation support for Cassandra based on {@link CassandraMappingContext} and {@link CassandraPersistentEntity}.
 * This class generates CQL to create user types (UDT) and tables.
 *
 * @author Mark Paluch
 * @author Jens Schauder
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
	 * Create tables from types known to {@link CassandraMappingContext}.
	 *
	 * @param ifNotExists {@literal true} to create tables using {@code IF NOT EXISTS}.
	 */
	public void createTables(boolean ifNotExists) {

		List<CreateTableSpecification> specifications = createTableSpecifications(ifNotExists);

		for (CreateTableSpecification specification : specifications) {
			cassandraAdminOperations.execute(CreateTableCqlGenerator.toCql(specification));
		}
	}

	/**
	 * Create {@link List} of {@link CreateTableSpecification}.
	 *
	 * @param ifNotExists {@literal true} to create tables using {@code IF NOT EXISTS}.
	 * @return {@link List} of {@link CreateTableSpecification}.
	 */
	protected List<CreateTableSpecification> createTableSpecifications(boolean ifNotExists) {

		Collection<? extends CassandraPersistentEntity<?>> entities = new ArrayList<CassandraPersistentEntity<?>>(
				mappingContext.getTableEntities());

		List<CreateTableSpecification> specifications = new ArrayList<CreateTableSpecification>();

		for (CassandraPersistentEntity<?> entity : entities) {
			specifications.add(mappingContext.getCreateTableSpecificationFor(entity).ifNotExists(ifNotExists));
		}

		return specifications;
	}

	/**
	 * Create user types from types known to {@link CassandraMappingContext}.
	 *
	 * @param ifNotExists {@literal true} to create types using {@code IF NOT EXISTS}.
	 */
	public void createUserTypes(boolean ifNotExists) {

		List<CreateUserTypeSpecification> specifications = createUserTypeSpecifications(ifNotExists);

		for (CreateUserTypeSpecification specification : specifications) {
			cassandraAdminOperations.execute(CreateUserTypeCqlGenerator.toCql(specification));
		}
	}

	/**
	 * Create {@link List} of {@link CreateUserTypeSpecification}.
	 *
	 * @param ifNotExists {@literal true} to create types using {@code IF NOT EXISTS}.
	 * @return {@link List} of {@link CreateUserTypeSpecification}.
	 */
	protected List<CreateUserTypeSpecification> createUserTypeSpecifications(boolean ifNotExists) {

		Collection<? extends CassandraPersistentEntity<?>> entities = new ArrayList<CassandraPersistentEntity<?>>(
				mappingContext.getUserDefinedTypeEntities());

		Map<CqlIdentifier, CassandraPersistentEntity<?>> byTableName = getEntitiesByTableName(entities);

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
					specifications.add(
							mappingContext.getCreateUserTypeSpecificationFor(byTableName.get(identifier)).ifNotExists(ifNotExists));
				}
			}
		}
		return specifications;
	}

	private Map<CqlIdentifier, CassandraPersistentEntity<?>> getEntitiesByTableName(
			Collection<? extends CassandraPersistentEntity<?>> entities) {

		// TODO simplify by using Java 8 Streams API in 2.0.x
		Map<CqlIdentifier, CassandraPersistentEntity<?>> byTableName = new HashMap<CqlIdentifier, CassandraPersistentEntity<?>>();

		for (CassandraPersistentEntity<?> entity : entities) {
			byTableName.put(entity.getTableName(), entity);
		}

		return byTableName;
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
}
