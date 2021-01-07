/*
 * Copyright 2017-2021 the original author or authors.
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

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

		this.cassandraAdminOperations.getKeyspaceMetadata() //
				.getTables() //
				.values() //
				.stream() //
				.map(RelationMetadata::getName) //
				.filter(table -> dropUnused || this.mappingContext.usesTable(table)) //
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

		Collection<UserDefinedType> userTypes = this.cassandraAdminOperations.getKeyspaceMetadata().getUserDefinedTypes()
				.values();

		getUserTypesToDrop(userTypes) //
				.stream() //
				.filter(it -> canRecreate.contains(it) || (dropUnused && !mappingContext.usesUserType(it))) //
				.forEach(this.cassandraAdminOperations::dropUserType);
	}

	/**
	 * Create {@link List} of {@link CqlIdentifier} with User-Defined type names to drop considering dependencies between
	 * UDTs.
	 *
	 * @return {@link List} of {@link CqlIdentifier}.
	 */
	private List<CqlIdentifier> getUserTypesToDrop(Collection<UserDefinedType> knownUserTypes) {

		List<CqlIdentifier> toDrop = new ArrayList<>();

		UserTypeDependencyGraphBuilder builder = new UserTypeDependencyGraphBuilder();
		knownUserTypes.forEach(builder::addUserType);

		UserTypeDependencyGraph dependencyGraph = builder.build();

		Set<CqlIdentifier> globalSeen = new LinkedHashSet<>();

		knownUserTypes.forEach(userType -> {
			toDrop.addAll(dependencyGraph.getDropOrder(userType.getName(), globalSeen::add));
		});

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
		private final MultiValueMap<CqlIdentifier, CqlIdentifier> dependencies = new LinkedMultiValueMap<>();

		/**
		 * Add {@link UserType} to the builder and inspect its dependencies.
		 *
		 * @param userType must not be {@literal null.}
		 */
		void addUserType(UserDefinedType userType) {

			Set<CqlIdentifier> seen = new LinkedHashSet<>();
			visitTypes(userType, seen::add);
		}

		/**
		 * Build the {@link UserTypeDependencyGraph}.
		 *
		 * @return the {@link UserTypeDependencyGraph}.
		 */
		UserTypeDependencyGraph build() {
			return new UserTypeDependencyGraph(new LinkedMultiValueMap<>(dependencies));
		}

		/**
		 * Visit a {@link UserType} and its fields.
		 *
		 * @param userType
		 * @param typeFilter
		 */
		private void visitTypes(UserDefinedType userType, Predicate<CqlIdentifier> typeFilter) {

			CqlIdentifier typeName = userType.getName();

			if (!typeFilter.test(typeName)) {
				return;
			}

			for (DataType fieldType : userType.getFieldTypes()) {

				if (fieldType instanceof UserDefinedType) {

					addDependency((UserDefinedType) fieldType, typeName, typeFilter);

					return;
				}

				doWithTypeArguments(fieldType, it -> {

					if (it instanceof UserDefinedType) {
						addDependency((UserDefinedType) it, typeName, typeFilter);
					}
				});
			}
		}

		private void addDependency(UserDefinedType userType, CqlIdentifier requiredBy,
				Predicate<CqlIdentifier> typeFilter) {

			dependencies.add(userType.getName(), requiredBy);

			visitTypes(userType, typeFilter);
		}

		private static void doWithTypeArguments(DataType type, Consumer<DataType> callback) {

			if (type instanceof MapType) {

				MapType mapType = (MapType) type;

				callback.accept(mapType.getKeyType());
				doWithTypeArguments(mapType.getKeyType(), callback);

				callback.accept(mapType.getValueType());
				doWithTypeArguments(mapType.getValueType(), callback);
			}

			if (type instanceof ListType) {

				ListType listType = (ListType) type;

				callback.accept(listType.getElementType());
				doWithTypeArguments(listType.getElementType(), callback);
			}

			if (type instanceof SetType) {

				SetType setType = (SetType) type;

				callback.accept(setType.getElementType());
				doWithTypeArguments(setType.getElementType(), callback);
			}

			if (type instanceof TupleType) {

				TupleType tupleType = (TupleType) type;

				tupleType.getComponentTypes().forEach(nested -> {
					callback.accept(nested);
					doWithTypeArguments(nested, callback);
				});
			}
		}
	}

	/**
	 * Dependency graph representing user type field dependencies to other user types.
	 *
	 * @author Mark Paluch
	 * @since 2.0.7
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

			List<CqlIdentifier> toDrop = new ArrayList<>();

			if (typeFilter.test(typeName)) {

				List<CqlIdentifier> dependants = dependencies.getOrDefault(typeName, Collections.emptyList());
				dependants.stream().map(dependant -> getDropOrder(dependant, typeFilter)).forEach(toDrop::addAll);

				toDrop.add(typeName);
			}

			return toDrop;
		}
	}
}
