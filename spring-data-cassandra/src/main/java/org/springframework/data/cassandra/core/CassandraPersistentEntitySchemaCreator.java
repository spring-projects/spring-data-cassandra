/*
 * Copyright 2016-2019 the original author or authors.
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
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
				.map(entity -> this.mappingContext.getCreateTableSpecificationFor(entity).ifNotExists(ifNotExists)) //
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
				.flatMap(entity -> this.mappingContext.getCreateIndexSpecificationsFor(entity).stream()) //
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

		Collection<? extends CassandraPersistentEntity<?>> entities = new ArrayList<>(
				this.mappingContext.getUserDefinedTypeEntities());

		Map<CqlIdentifier, CassandraPersistentEntity<?>> byTableName = entities.stream()
				.collect(Collectors.toMap(CassandraPersistentEntity::getTableName, entity -> entity));

		List<CreateUserTypeSpecification> specifications = new ArrayList<>();
		UserDefinedTypeSet udts = new UserDefinedTypeSet();

		entities.forEach(entity -> {

			udts.add(entity.getTableName());
			visitUserTypes(entity, udts);
		});

		specifications
				.addAll(udts
						.stream().map(identifier -> this.mappingContext
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
				} else {
					udts.updateDependency(entity.getTableName(), propertyType.getTableName());
				}
			}
		}
	}

	/**
	 * Object to record dependencies and report them in the order of creation.
	 */
	static class UserDefinedTypeSet implements Streamable<CqlIdentifier> {

		private final Set<CqlIdentifier> seen = new HashSet<>();
		private final List<CqlIdentifier> creationOrder = new ArrayList<>();

		public boolean add(CqlIdentifier cqlIdentifier) {

			if (seen.add(cqlIdentifier)) {
				creationOrder.add(cqlIdentifier);
				return true;
			}

			return false;
		}

		@NotNull
		@Override
		public Iterator<CqlIdentifier> iterator() {

			List<CqlIdentifier> reverseCreationOrder = new ArrayList<>(creationOrder);
			Collections.reverse(reverseCreationOrder);

			return reverseCreationOrder.iterator();
		}

		/**
		 * Checks the dependency order. {@code referent} depends on {@code reference} and we need to make sure that
		 * {@code referent} gets created after {@code reference}.
		 * <p>
		 * This method therefore updates the dependency order.
		 *
		 * @param referent the client of {@code reference}.
		 * @param reference the dependency required by {@code referent}.
		 */
		void updateDependency(CqlIdentifier referent, CqlIdentifier reference) {

			int referentIndex = creationOrder.indexOf(referent);
			int referenceIndex = creationOrder.indexOf(reference);

			if (referentIndex > referenceIndex) {

				creationOrder.remove(referent);
				creationOrder.add(referenceIndex, referent);
			}
		}
	}
}
