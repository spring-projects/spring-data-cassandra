/*
 * Copyright 2013-2014 the original author or authors
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

import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.data.mapping.context.MappingContext;

import com.datastax.driver.core.TableMetadata;

/**
 * A {@link MappingContext} for Cassandra.
 * 
 * @author Matthew T. Adams
 */
public interface CassandraMappingContext extends
		MappingContext<CassandraPersistentEntity<?>, CassandraPersistentProperty> {

	/**
	 * Returns only those entities that don't represent primary key types.
	 * 
	 * @see #getPersistentEntities(boolean)
	 */
	@Override
	public Collection<CassandraPersistentEntity<?>> getPersistentEntities();

	/**
	 * Returns all persistent entities or only non-primary-key entities.
	 * 
	 * @param includePrimaryKeyTypes If <code>true</code>, returns all entities, including entities that represent primary
	 *          key types. If <code>false</code>, returns only entities that don't represent primary key types.
	 */
	public Collection<CassandraPersistentEntity<?>> getPersistentEntities(boolean includePrimaryKeyTypes);

	/**
	 * Returns only those entities representing primary key types.
	 */
	Collection<CassandraPersistentEntity<?>> getPrimaryKeyEntities();

	/**
	 * Returns only those entities not representing primary key types.
	 * 
	 * @see #getPersistentEntities(boolean)
	 */
	Collection<CassandraPersistentEntity<?>> getNonPrimaryKeyEntities();

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 * 
	 * @param The entity. May not be null.
	 */
	CreateTableSpecification getCreateTableSpecificationFor(CassandraPersistentEntity<?> entity);

	/**
	 * Returns whether this mapping context has any entities mapped to the given table.
	 * 
	 * @param table May not be null.
	 */
	boolean usesTable(TableMetadata table);

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
}
