/*
 * Copyright 2017-2018 the original author or authors.
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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.UserType.Field;

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

		Set<CqlIdentifier> canRecreate = new HashSet<CqlIdentifier>();

		for (CassandraPersistentEntity<?> entity : this.mappingContext.getUserDefinedTypeEntities()) {
			canRecreate.add(entity.getTableName());
		}

		Collection<UserType> userTypes = this.cassandraAdminOperations.getKeyspaceMetadata().getUserTypes();

		List<CqlIdentifier> identifiers = getUserTypesToDrop(userTypes);

		for (CqlIdentifier identifier : identifiers) {

			if (canRecreate.contains(identifier) || (dropUnused && !mappingContext.usesUserType(identifier))) {
				this.cassandraAdminOperations.dropUserType(identifier);
			}
		}
	}

	/**
	 * Create {@link List} of {@link CqlIdentifier} with User-Defined type names to drop considering dependencies between
	 * UDTs.
	 *
	 * @return {@link List} of {@link CqlIdentifier}.
	 */
	private List<CqlIdentifier> getUserTypesToDrop(Collection<UserType> knownUserTypes) {

		List<CqlIdentifier> toDrop = new ArrayList<CqlIdentifier>();

		UserTypeDependencyGraphBuilder builder = new UserTypeDependencyGraphBuilder();

		for (UserType userType : knownUserTypes) {
			builder.addUserType(userType);
		}

		UserTypeDependencyGraph dependencyGraph = builder.build();

		final Set<CqlIdentifier> globalSeen = new LinkedHashSet<CqlIdentifier>();

		for (UserType userType : knownUserTypes) {

			CqlIdentifier typeName = CqlIdentifier.cqlId(userType.getTypeName());
			toDrop.addAll(dependencyGraph.getDropOrder(typeName, new Predicate<CqlIdentifier>() {
				@Override
				public boolean test(CqlIdentifier type) {
					return globalSeen.add(type);
				}
			}));
		}

		return toDrop;
	}

	/**
	 * Builder for {@link UserTypeDependencyGraph}. Introspects {@link UserType} for dependencies to other user types to
	 * build a dependency graph between user types.
	 *
	 * @author Mark Paluch
	 * @since 2.0.7
	 */
	static class UserTypeDependencyGraphBuilder {

		// Maps user types to other types they are referenced in.
		private final MultiValueMap<CqlIdentifier, CqlIdentifier> dependencies = new LinkedMultiValueMap<CqlIdentifier, CqlIdentifier>();

		/**
		 * Add {@link UserType} to the builder and inspect its dependencies.
		 *
		 * @param userType must not be {@literal null.}
		 */
		void addUserType(UserType userType) {

			final Set<CqlIdentifier> seen = new LinkedHashSet<CqlIdentifier>();

			visitTypes(userType, new Predicate<CqlIdentifier>() {
				@Override
				public boolean test(CqlIdentifier type) {
					return seen.add(type);
				}
			});
		}

		/**
		 * Build the {@link UserTypeDependencyGraph}.
		 *
		 * @return the {@link UserTypeDependencyGraph}.
		 */
		UserTypeDependencyGraph build() {
			return new UserTypeDependencyGraph(new LinkedMultiValueMap<CqlIdentifier, CqlIdentifier>(dependencies));
		}

		/**
		 * Visit a {@link UserType} and its fields.
		 *
		 * @param userType
		 * @param typeFilter
		 */
		private void visitTypes(UserType userType, final Predicate<CqlIdentifier> typeFilter) {

			final CqlIdentifier typeName = CqlIdentifier.cqlId(userType.getTypeName());

			if (!typeFilter.test(typeName)) {
				return;
			}

			for (Field field : userType) {

				if (field.getType() instanceof UserType) {

					addDependency((UserType) field.getType(), typeName, typeFilter);

					return;
				}

				doWithTypeArguments(field.getType(), new Consumer<DataType>() {
					@Override
					public void accept(DataType type) {
						if (type instanceof UserType) {
							addDependency((UserType) type, typeName, typeFilter);
						}
					}
				});
			}
		}

		private void addDependency(UserType userType, CqlIdentifier requiredBy, Predicate<CqlIdentifier> typeFilter) {

			dependencies.add(CqlIdentifier.cqlId(userType.getTypeName()), requiredBy);

			visitTypes(userType, typeFilter);
		}

		private static void doWithTypeArguments(DataType type, Consumer<DataType> callback) {

			for (DataType nested : type.getTypeArguments()) {
				callback.accept(nested);
				doWithTypeArguments(nested, callback);
			}

			if (type instanceof TupleType) {

				TupleType tupleType = (TupleType) type;

				for (DataType nested : tupleType.getComponentTypes()) {
					callback.accept(nested);
					doWithTypeArguments(nested, callback);
				}
			}
		}
	}

	/**
	 * Dependency graph representing user type field dependencies to other user types.
	 *
	 * @author Mark Paluch
	 * @since 1.5.12
	 */
	static class UserTypeDependencyGraph {

		private final MultiValueMap<CqlIdentifier, CqlIdentifier> dependencies;

		UserTypeDependencyGraph(MultiValueMap<CqlIdentifier, CqlIdentifier> dependencies) {
			this.dependencies = dependencies;
		}

		/**
		 * Returns the names of user types in the order they need to be dropped including type {@code typeName}.
		 *
		 * @param typeName
		 * @param typeFilter
		 * @return
		 */
		List<CqlIdentifier> getDropOrder(CqlIdentifier typeName, Predicate<CqlIdentifier> typeFilter) {

			List<CqlIdentifier> toDrop = new ArrayList<CqlIdentifier>();

			if (typeFilter.test(typeName)) {

				List<CqlIdentifier> dependants = dependencies.get(typeName);
				if (dependants != null) {

					for (CqlIdentifier dependant : dependants) {
						toDrop.addAll(getDropOrder(dependant, typeFilter));
					}
				}

				toDrop.add(typeName);
			}

			return toDrop;
		}
	}

	/**
	 * Represents a predicate (boolean-valued function) of one argument.
	 *
	 * @param <T>
	 */
	interface Predicate<T> {

		boolean test(T type);
	}

	/**
	 * Represents an operation that accepts a single input argument and returns no result. Unlike most other functional
	 * interfaces, {@code Consumer} is expected to operate via side-effects.
	 *
	 * @param <T>
	 */
	interface Consumer<T> {

		void accept(T type);
	}
}
