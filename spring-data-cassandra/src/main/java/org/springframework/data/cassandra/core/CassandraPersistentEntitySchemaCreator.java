/*
 * Copyright 2016-2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

import org.springframework.data.cassandra.core.cql.generator.CreateIndexCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.CreateUserTypeCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Schema creation support for Cassandra based on {@link CassandraMappingContext} and {@link CassandraPersistentEntity}.
 * This class generates CQL to create user types (UDT) and tables.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.5
 * @see org.springframework.data.cassandra.core.mapping.Table
 * @see org.springframework.data.cassandra.core.mapping.UserDefinedType
 * @see org.springframework.data.cassandra.core.mapping.CassandraType
 */
public class CassandraPersistentEntitySchemaCreator {

	private final CassandraAdminOperations cassandraAdminOperations;

	private final CassandraMappingContext mappingContext;

	/**
	 * Create a new {@link CassandraPersistentEntitySchemaCreator} for the given {@link CassandraMappingContext} and
	 * {@link CassandraAdminOperations}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param cassandraAdminOperations must not be {@literal null}.
	 * @since 3.0
	 */
	public CassandraPersistentEntitySchemaCreator(CassandraAdminOperations cassandraAdminOperations) {

		Assert.notNull(cassandraAdminOperations, "CassandraAdminOperations must not be null");

		this.cassandraAdminOperations = cassandraAdminOperations;
		this.mappingContext = cassandraAdminOperations.getConverter().getMappingContext();
	}

	/**
	 * Create a new {@link CassandraPersistentEntitySchemaCreator} for the given {@link CassandraMappingContext} and
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

		createTableSpecifications(ifNotExists).stream() //
				.map(CreateTableCqlGenerator::toCql) //
				.forEach(cql -> this.cassandraAdminOperations.getCqlOperations().execute(cql));
	}

	/**
	 * Create {@link List} of {@link CreateTableSpecification}.
	 *
	 * @param ifNotExists {@literal true} to create tables using {@code IF NOT EXISTS}.
	 * @return {@link List} of {@link CreateTableSpecification}.
	 */
	protected List<CreateTableSpecification> createTableSpecifications(boolean ifNotExists) {

		return this.mappingContext.getTableEntities() //
				.stream() //
				.map(entity -> cassandraAdminOperations.getSchemaFactory().getCreateTableSpecificationFor(entity)
						.ifNotExists(ifNotExists)) //
				.collect(Collectors.toList());
	}

	/**
	 * Create indexes from types known to {@link CassandraMappingContext}.
	 *
	 * @param ifNotExists {@literal true} to create tables using {@code IF NOT EXISTS}.
	 */
	public void createIndexes(boolean ifNotExists) {

		createIndexSpecifications(ifNotExists).stream() //
				.map(CreateIndexCqlGenerator::toCql) //
				.forEach(cql -> this.cassandraAdminOperations.getCqlOperations().execute(cql));
	}

	/**
	 * Create {@link List} of {@link CreateIndexSpecification}.
	 *
	 * @param ifNotExists {@literal true} to create indexes using {@code IF NOT EXISTS}.
	 * @return {@link List} of {@link CreateIndexSpecification}.
	 */
	protected List<CreateIndexSpecification> createIndexSpecifications(boolean ifNotExists) {

		return this.mappingContext.getTableEntities() //
				.stream() //
				.flatMap(entity -> cassandraAdminOperations.getSchemaFactory().getCreateIndexSpecificationsFor(entity).stream()) //
				.peek(it -> it.ifNotExists(ifNotExists)) //
				.collect(Collectors.toList());
	}

	/**
	 * Create user types from types known to {@link CassandraMappingContext}.
	 *
	 * @param ifNotExists {@literal true} to create types using {@code IF NOT EXISTS}.
	 */
	public void createUserTypes(boolean ifNotExists) {

		createUserTypeSpecifications(ifNotExists).stream() //
				.map(CreateUserTypeCqlGenerator::toCql) //
				.forEach(cql -> this.cassandraAdminOperations.getCqlOperations().execute(cql));
	}

	/**
	 * Create {@link List} of {@link CreateUserTypeSpecification}.
	 *
	 * @param ifNotExists {@literal true} to create types using {@code IF NOT EXISTS}.
	 * @return {@link List} of {@link CreateUserTypeSpecification}.
	 */
	protected List<CreateUserTypeSpecification> createUserTypeSpecifications(boolean ifNotExists) {

		List<? extends CassandraPersistentEntity<?>> entities = new ArrayList<>(
				this.mappingContext.getUserDefinedTypeEntities());

		Map<CqlIdentifier, CassandraPersistentEntity<?>> byTableName = entities.stream()
				.collect(Collectors.toMap(CassandraPersistentEntity::getTableName, entity -> entity));

		List<CreateUserTypeSpecification> specifications = new ArrayList<>();
		UserDefinedTypeSet udts = new UserDefinedTypeSet();

		entities.forEach(entity -> {
			udts.add(entity.getTableName());
			visitUserTypes(entity, udts);
		});

		specifications.addAll(udts.stream()
				.map(identifier -> cassandraAdminOperations.getSchemaFactory()
						.getCreateUserTypeSpecificationFor(byTableName.get(identifier)).ifNotExists(ifNotExists))
				.collect(Collectors.toList()));

		return specifications;
	}

	private void visitUserTypes(CassandraPersistentEntity<?> entity, UserDefinedTypeSet udts) {

		for (CassandraPersistentProperty property : entity) {

			BasicCassandraPersistentEntity<?> propertyType = this.mappingContext.getPersistentEntity(property);

			if (propertyType == null) {
				continue;
			}

			if (propertyType.isUserDefinedType()) {
				if (udts.add(propertyType.getTableName())) {
					visitUserTypes(propertyType, udts);
				}
				udts.addDependency(entity.getTableName(), propertyType.getTableName());
			}
		}
	}

	/**
	 * Object to record dependencies and report them in the order of creation.
	 */
	static class UserDefinedTypeSet implements Streamable<CqlIdentifier> {

		private final Set<CqlIdentifier> seen = new HashSet<>();
		private final List<DependencyNode> creationOrder = new ArrayList<>();

		public boolean add(CqlIdentifier cqlIdentifier) {

			if (seen.add(cqlIdentifier)) {
				creationOrder.add(new DependencyNode(cqlIdentifier));
				return true;
			}

			return false;
		}

		@NotNull
		@Override
		public Iterator<CqlIdentifier> iterator() {

			// Return items in creation order considering dependencies
			return creationOrder.stream() //
					.sorted((left, right) -> {

						if (left.dependsOn(right.getIdentifier())) {
							return 1;
						}

						if (right.dependsOn(left.getIdentifier())) {
							return -1;
						}

						return 0;
					}) //
					.map(DependencyNode::getIdentifier) //
					.iterator();
		}

		/**
		 * Updates the dependency order.
		 *
		 * @param typeToCreate the client of {@code dependsOn}.
		 * @param dependsOn the dependency required by {@code typeToCreate}.
		 */
		void addDependency(CqlIdentifier typeToCreate, CqlIdentifier dependsOn) {

			for (DependencyNode toCreate : creationOrder) {
				if (toCreate.matches(typeToCreate)) {
					toCreate.addDependency(dependsOn);
				}
			}
		}

		static class DependencyNode {

			private final CqlIdentifier identifier;
			private final List<CqlIdentifier> dependsOn = new ArrayList<>();

			DependencyNode(CqlIdentifier identifier) {
				this.identifier = identifier;
			}

			public CqlIdentifier getIdentifier() {
				return identifier;
			}

			boolean matches(CqlIdentifier typeToCreate) {
				return identifier.equals(typeToCreate);
			}

			void addDependency(CqlIdentifier dependsOn) {
				this.dependsOn.add(dependsOn);
			}

			boolean dependsOn(CqlIdentifier identifier) {
				return this.dependsOn.contains(identifier);
			}
		}
	}
}
