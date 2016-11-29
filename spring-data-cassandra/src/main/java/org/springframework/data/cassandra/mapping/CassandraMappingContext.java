/*
 * Copyright 2013-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.mapping;

import java.util.Collection;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;

import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;

/**
 * A {@link MappingContext} for Cassandra.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public interface CassandraMappingContext
		extends MappingContext<CassandraPersistentEntity<?>, CassandraPersistentProperty> {

	/**
	 * Returns only those entities that don't represent primary key types.
	 *
	 * @see #getPersistentEntities(boolean)
	 */
	@Override
	Collection<CassandraPersistentEntity<?>> getPersistentEntities();

	/**
	 * Returns all persistent entities or only non-primary-key entities.
	 *
	 * @param includePrimaryKeyTypesAndUdts If {@literal true}, returns all entities, including entities that represent primary
	 *          key types and user-defined types. If {@literal false}, returns only entities that don't represent primary key types and no user-defined types.
	 */
	Collection<CassandraPersistentEntity<?>> getPersistentEntities(boolean includePrimaryKeyTypesAndUdts);

	/**
	 * Returns only those entities not representing primary key types.
	 *
	 * @see #getPersistentEntities(boolean)
	 * @deprecated as of 1.5, use {@link #getTableEntities()}.
	 */
	@Deprecated
	Collection<CassandraPersistentEntity<?>> getNonPrimaryKeyEntities();

	/**
	 * Returns only those entities representing primary key types.
	 * @deprecated as of 1.5
	 */
	@Deprecated
	Collection<CassandraPersistentEntity<?>> getPrimaryKeyEntities();

	/**
	 * Returns only {@link Table} entities.
	 *
	 * @since 1.5
	 */
	Collection<CassandraPersistentEntity<?>> getTableEntities();

	/**
	 * Returns only those entities representing a user defined type.
	 *
	 * @see #getPersistentEntities(boolean)
	 * @since 1.5
	 */
	Collection<CassandraPersistentEntity<?>> getUserDefinedTypeEntities();

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 */
	CreateTableSpecification getCreateTableSpecificationFor(CassandraPersistentEntity<?> entity);

	/**
	 * Returns a {@link CreateUserTypeSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 */
	CreateUserTypeSpecification getCreateUserTypeSpecificationFor(CassandraPersistentEntity<?> entity);

	/**
	 * Returns whether this mapping context has any entities mapped to the given table.
	 *
	 * @param table must not be {@literal null}.
	 * @return @return {@literal true} is this {@literal TableMetadata} is used by a mapping.
	 */
	boolean usesTable(TableMetadata table);

	/**
	 * Returns whether this mapping context has any entities using the given user type.
	 *
	 * @param userType must not be {@literal null}.
	 * @return {@literal true} is this {@literal UserType} is used.
	 * @since 1.5
	 */
	boolean usesUserType(UserType userType);

	/**
	 * Returns the existing {@link CassandraPersistentEntity} for the given {@link Class}. If it is not yet known to this
	 * {@link CassandraMappingContext}, an {@link IllegalArgumentException} is thrown.
	 *
	 * @param type The class of the existing persistent entity.
	 * @return The existing persistent entity.
	 */
	CassandraPersistentEntity<?> getExistingPersistentEntity(Class<?> type);

	/**
	 * Returns whether this {@link CassandraMappingContext} already contains a {@link CassandraPersistentEntity} for the
	 * given type.
	 */
	boolean contains(Class<?> type);

	/**
	 * Sets a verifier other than the {@link BasicCassandraPersistentEntityMetadataVerifier}
	 */
	void setVerifier(CassandraPersistentEntityMetadataVerifier verifier);

	/**
	 * Retrieve the data type of the property. Cassandra {@link DataType types} are determined using simple types and
	 * configured {@link CustomConversions}.
	 *
	 * @param property must not be {@literal null}.
	 * @return the Cassandra {@link DataType type}.
	 * @see CustomConversions
	 * @see CassandraSimpleTypeHolder
	 * @since 1.5
	 */
	DataType getDataType(CassandraPersistentProperty property);

	/**
	 * Retrieve the data type based on the given {@code type}. Cassandra {@link DataType types} are determined using simple types and
	 * configured {@link CustomConversions}.
	 *
	 * @param type must not be {@literal null}.
	 * @return the Cassandra {@link DataType type}.
	 * @see CustomConversions
	 * @see CassandraSimpleTypeHolder
	 * @since 1.5
	 */
	DataType getDataType(Class<?> type);

}
