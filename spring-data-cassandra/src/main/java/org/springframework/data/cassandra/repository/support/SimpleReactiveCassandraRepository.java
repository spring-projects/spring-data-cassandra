/*
 * Copyright 2016-2018 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import org.reactivestreams.Publisher;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Reactive repository base implementation for Cassandra.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class SimpleReactiveCassandraRepository<T, ID> implements ReactiveCassandraRepository<T, ID> {

	private final CassandraEntityInformation<T, ID> entityInformation;

	private final ReactiveCassandraOperations operations;

	/**
	 * Create a new {@link SimpleReactiveCassandraRepository} for the given {@link CassandraEntityInformation} and
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

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(S)
	 */
	@Override
	public <S extends T> Mono<S> save(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		return operations.getReactiveCqlOperations().execute(createInsert(entity)).map(it -> entity);
	}

	/**
	 * Create a {@link Insert} statement containing all properties including these with {@literal null} values.
	 *
	 * @param entity the entity, must not be {@literal null}.
	 * @return the constructed {@link Insert} statement.
	 */
	private <S extends T> Insert createInsert(S entity) {
		return InsertUtil.createInsert(operations.getConverter(), entity);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		return saveAll(Flux.fromIterable(entities));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(org.reactivestreams.Publisher)
	 */
	@Override
	public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null");

		return Flux.from(entityStream)
				.flatMap(entity -> operations.getReactiveCqlOperations().execute(createInsert(entity)).map(it -> entity));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.ReactiveCassandraRepository#insert(java.lang.Object)
	 */
	@Override
	public <S extends T> Mono<S> insert(S entity) {

		Assert.notNull(entity, "Entity must not be null");

		return operations.insert(entity);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.ReactiveCassandraRepository#insert(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Flux<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		return Flux.fromIterable(entities).flatMap(operations::insert);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.ReactiveCassandraRepository#insert(org.reactivestreams.Publisher)
	 */
	@Override
	public <S extends T> Flux<S> insert(Publisher<S> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null");

		return Flux.from(entityStream).flatMap(operations::insert);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#count()
	 */
	@Override
	public Mono<Long> count() {
		return operations.count(entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> existsById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return operations.exists(id, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Boolean> existsById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "The Publisher of ids must not be null");

		return Mono.from(publisher).flatMap(this::existsById);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(java.lang.Object)
	 */
	@Override
	public Mono<T> findById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return operations.selectOneById(id, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<T> findById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "The Publisher of ids must not be null");

		return Mono.from(publisher).flatMap(this::findById);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAll()
	 */
	@Override
	public Flux<T> findAll() {

		Select select = QueryBuilder.select().from(entityInformation.getTableName().toCql());

		return operations.select(select, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(java.lang.Iterable)
	 */
	@Override
	public Flux<T> findAllById(Iterable<ID> iterable) {

		Assert.notNull(iterable, "The given Iterable of ids must not be null");

		return findAllById(Flux.fromIterable(iterable));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<T> findAllById(Publisher<ID> idStream) {

		Assert.notNull(idStream, "The given Publisher of ids must not be null");

		return Flux.from(idStream).flatMap(this::findById);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#delete(java.lang.Object)
	 */
	@Override
	public Mono<Void> delete(T entity) {

		Assert.notNull(entity, "The given entity must not be null");

		return operations.delete(entity).then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(java.lang.Object)
	 */
	@Override
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "The given id must not be null");

		return operations.deleteById(id, entityInformation.getJavaType()).then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Void> deleteById(Publisher<ID> publisher) {

		Assert.notNull(publisher, "The Publisher of ids must not be null");

		return Mono.from(publisher).flatMap(this::deleteById).then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll()
	 */
	@Override
	public Mono<Void> deleteAll() {
		return operations.truncate(entityInformation.getJavaType());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(java.lang.Iterable)
	 */
	@Override
	public Mono<Void> deleteAll(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null");

		return Flux.fromIterable(entities).flatMap(operations::delete).then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {

		Assert.notNull(entityStream, "The given Publisher of entities must not be null");

		return Flux.from(entityStream).flatMap(operations::delete).then();
	}
}
