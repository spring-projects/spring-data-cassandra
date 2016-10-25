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
package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;

import org.reactivestreams.Publisher;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive repository base implementation for Cassandra.
 * 
 * @author Mark Paluch
 * @since 2.0
 */
public class SimpleReactiveCassandraRepository<T, ID extends Serializable>
		implements ReactiveCassandraRepository<T, ID> {

	protected ReactiveCassandraOperations operations;
	protected CassandraEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleReactiveCassandraRepository} for the given {@link CassandraEntityInformation} and
	 * {@link ReactiveCassandraOperations}.
	 *
	 * @param metadata must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public SimpleReactiveCassandraRepository(CassandraEntityInformation<T, ID> metadata,
			ReactiveCassandraOperations operations) {

		Assert.notNull(metadata, "CassandraEntityInformation must not be null");
		Assert.notNull(operations, "ReactiveCassandraOperations must not be null");

		this.entityInformation = metadata;
		this.operations = operations;
	}

	@Override
	public <S extends T> Mono<S> save(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		if (entityInformation.isNew(entity)) {
			return operations.insert(entity);
		}

		return operations.update(entity);

	}

	@Override
	public <S extends T> Flux<S> save(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		return save(Flux.fromIterable(entities));
	}

	@Override
	public <S extends T> Flux<S> save(Publisher<S> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null");

		return Flux.from(entityStream).flatMap(entity -> {

			if (entityInformation.isNew(entity)) {
				return operations.insert(entity);
			}

			return operations.update(entity);
		});
	}

	@Override
	public <S extends T> Mono<S> insert(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		return operations.insert(entity);
	}

	@Override
	public <S extends T> Flux<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		return operations.insert(Flux.fromIterable(entities));
	}

	@Override
	public <S extends T> Flux<S> insert(Publisher<S> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null");

		return operations.insert(entityStream);
	}

	@Override
	public Mono<T> findOne(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return operations.selectOneById(id, entityInformation.getJavaType());
	}

	@Override
	public Mono<T> findOne(Mono<ID> mono) {

		Assert.notNull(mono, "The given id must not be null");

		return mono.then(id -> operations.selectOneById(id, entityInformation.getJavaType()));
	}

	@Override
	public Mono<Boolean> exists(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return operations.exists(id, entityInformation.getJavaType());
	}

	@Override
	public Mono<Boolean> exists(Mono<ID> mono) {

		Assert.notNull(mono, "The given id must not be null");

		return mono.then(id -> operations.exists(id, entityInformation.getJavaType()));
	}

	@Override
	public Flux<T> findAll() {

		Select select = QueryBuilder.select().from(entityInformation.getTableName().toCql());
		return operations.select(select, entityInformation.getJavaType());
	}

	@Override
	public Flux<T> findAll(Iterable<ID> iterable) {

		Assert.notNull(iterable, "The given Iterable of id's must not be null");

		return findAll(Flux.fromIterable(iterable));
	}

	@Override
	public Flux<T> findAll(Publisher<ID> idStream) {

		Assert.notNull(idStream, "The given Publisher of id's must not be null");

		return Flux.from(idStream).flatMap(id -> operations.selectOneById(id, entityInformation.getJavaType()));
	}

	@Override
	public Mono<Long> count() {
		return operations.count(entityInformation.getJavaType());
	}

	@Override
	public Mono<Void> delete(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return operations.deleteById(id, entityInformation.getJavaType()).then();
	}

	@Override
	public Mono<Void> delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null");

		return operations.delete(entity).then();
	}

	@Override
	public Mono<Void> delete(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		return operations.delete(Flux.fromIterable(entities)).then();
	}

	@Override
	public Mono<Void> delete(Publisher<? extends T> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null");

		return operations.delete(entityStream).then();
	}

	@Override
	public Mono<Void> deleteAll() {
		return operations.truncate(entityInformation.getJavaType());
	}
}
