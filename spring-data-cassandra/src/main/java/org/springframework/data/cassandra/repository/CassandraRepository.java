/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository;

import java.util.List;

import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Persistable;
import org.springframework.data.domain.Slice;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Cassandra-specific extension of the {@link CrudRepository} interface that allows the specification of a type for the
 * identity of the {@link Table @Table} (or {@link Persistable @Persistable}) type.
 * <p />
 * Repositories based on {@link CassandraRepository} can define either a single primary key, use a primary key class or
 * a compound primary key without a primary key class. Types using a compound primary key without a primary key class
 * must use {@link MapId} to declare their key value.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see MapIdCassandraRepository
 */
@NoRepositoryBean
public interface CassandraRepository<T, ID> extends CrudRepository<T, ID> {

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#saveAll(java.lang.Iterable)
	 */
	@Override
	<S extends T> List<S> saveAll(Iterable<S> entites);

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	@Override
	List<T> findAll();

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Note: Cassandra supports single-field {@code IN} queries only. When using {@link MapId} with multiple components,
	 * use {@link #findById(Object)}.
	 *
	 * @throws org.springframework.dao.InvalidDataAccessApiUsageException thrown when using {@link MapId} with multiple
	 *           key components.
	 */
	@Override
	List<T> findAllById(Iterable<ID> ids);

	/**
	 * Returns a {@link Slice} of entities meeting the paging restriction provided in the {@code Pageable} object.
	 *
	 * @param pageable must not be {@literal null}.
	 * @return a {@link Slice} of entities.
	 * @see CassandraPageRequest
	 * @since 2.0
	 */
	Slice<T> findAll(Pageable pageable);

	/**
	 * Inserts the given entity. Assumes the instance to be new to be able to apply insertion optimizations. Use the
	 * returned instance for further operations as the save operation might have changed the entity instance completely.
	 * Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the saved entity
	 * @since 2.0
	 */
	<S extends T> S insert(S entity);

	/**
	 * Inserts the given entities. Assumes the given entities to have not been persisted yet and thus will optimize the
	 * insert over a call to {@link #saveAll(Iterable)}. Prefer using {@link #saveAll(Iterable)} to avoid the usage of
	 * store specific API.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the saved entities
	 * @since 2.0
	 */
	<S extends T> List<S> insert(Iterable<S> entities);

}
