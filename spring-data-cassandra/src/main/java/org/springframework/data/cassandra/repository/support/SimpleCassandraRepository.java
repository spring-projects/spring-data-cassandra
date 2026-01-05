/*
 * Copyright 2013-present the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import static org.springframework.data.cassandra.core.query.Criteria.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.util.Assert;

/**
 * Repository base implementation for Cassandra.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author John Blum
 * @author Jens Schauder
 * @see org.springframework.data.cassandra.repository.CassandraRepository
 */
public class SimpleCassandraRepository<T, ID> implements CassandraRepository<T, ID> {

	private static final InsertOptions INSERT_NULLS = InsertOptions.builder().withInsertNulls().build();

	private final AbstractMappingContext<BasicCassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final CassandraEntityInformation<T, ID> entityInformation;

	private final CassandraOperations operations;

	/**
	 * Create a new {@link SimpleCassandraRepository} for the given {@link CassandraEntityInformation} and
	 * {@link CassandraTemplate}.
	 *
	 * @param metadata must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public SimpleCassandraRepository(CassandraEntityInformation<T, ID> metadata, CassandraOperations operations) {

		Assert.notNull(metadata, "CassandraEntityInformation must not be null");
		Assert.notNull(operations, "CassandraOperations must not be null");

		this.entityInformation = metadata;
		this.operations = operations;
		this.mappingContext = operations.getConverter().getMappingContext();
	}

	// -------------------------------------------------------------------------
	// Methods from CrudRepository
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> S save(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		BasicCassandraPersistentEntity<?> persistentEntity = this.mappingContext.getPersistentEntity(entity.getClass());

		if (persistentEntity != null && persistentEntity.hasVersionProperty()) {

			if (!entityInformation.isNew(entity)) {
				return this.operations.update(entity);
			}
		}

		return this.operations.insert(entity, INSERT_NULLS).getEntity();
	}

	@Override
	public <S extends T> List<S> saveAll(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		List<S> result = new ArrayList<>();

		for (S entity : entities) {
			result.add(save(entity));
		}

		return result;
	}

	@Override
	public Optional<T> findById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return Optional.ofNullable(doFindOne(id));
	}

	private @Nullable T doFindOne(ID id) {
		return this.operations.selectOneById(id, this.entityInformation.getJavaType());
	}

	@Override
	public boolean existsById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return this.operations.exists(id, this.entityInformation.getJavaType());
	}

	@Override
	public List<T> findAll() {
		return this.operations.select(Query.empty(), this.entityInformation.getJavaType());
	}

	@Override
	public List<T> findAllById(Iterable<ID> ids) {

		Assert.notNull(ids, "The given Iterable of id's must not be null");

		if (!ids.iterator().hasNext()) {
			return Collections.emptyList();
		}

		return this.operations.select(createIdsInQuery(ids), this.entityInformation.getJavaType());
	}

	@Override
	public long count() {
		return this.operations.count(this.entityInformation.getJavaType());
	}

	@Override
	public void deleteById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		this.operations.deleteById(id, this.entityInformation.getJavaType());
	}

	@Override
	public void delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null");

		deleteById(this.entityInformation.getRequiredId(entity));
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> ids) {

		Assert.notNull(ids, "The given Iterable of ids must not be null");

		if (FindByIdQuery.hasCompositeKeys(ids, this.mappingContext)) {

			for (ID id : ids) {
				deleteById(id);
			}
			return;
		}

		this.operations.delete(createIdsInQuery(ids), this.entityInformation.getJavaType());
	}

	@Override
	public void deleteAll(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		entities.forEach(this.operations::delete);
	}

	@Override
	public void deleteAll() {
		this.operations.truncate(this.entityInformation.getJavaType());
	}

	// -------------------------------------------------------------------------
	// Methods from CassandraRepository
	// -------------------------------------------------------------------------

	@Override
	public Slice<T> findAll(Pageable pageable) {

		Assert.notNull(pageable, "Pageable must not be null");

		return this.operations.slice(Query.empty().pageRequest(pageable), this.entityInformation.getJavaType());
	}

	@Override
	public <S extends T> S insert(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		return this.operations.insert(entity);
	}

	@Override
	public <S extends T> List<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		List<S> result = new ArrayList<>();

		for (S entity : entities) {
			result.add(this.operations.insert(entity));
		}

		return result;
	}

	private Query createIdsInQuery(Iterable<? extends ID> ids) {

		FindByIdQuery mapIdQuery = FindByIdQuery.forIds(ids);
		List<Object> idCollection = mapIdQuery.getIdCollection();
		String idField = mapIdQuery.getIdProperty();

		if (idField == null) {
			idField = this.entityInformation.getIdAttribute();
		}

		return Query.query(where(idField).in(idCollection));
	}

}
