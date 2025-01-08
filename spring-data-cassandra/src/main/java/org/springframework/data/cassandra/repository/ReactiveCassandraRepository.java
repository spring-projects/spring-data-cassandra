/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.cassandra.repository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

/**
 * Cassandra specific {@link org.springframework.data.repository.Repository} interface with reactive support.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@NoRepositoryBean
public interface ReactiveCassandraRepository<T, ID> extends ReactiveCrudRepository<T, ID> {

	/**
	 * Inserts the given entity. Assumes the instance to be new to be able to apply insertion optimizations. Use the
	 * returned instance for further operations as the save operation might have changed the entity instance completely.
	 * Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Mono<S> insert(S entity);

	/**
	 * Inserts the given entities. Assumes the instance to be new to be able to apply insertion optimizations. Use the
	 * returned instance for further operations as the save operation might have changed the entity instance completely.
	 * Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Flux<S> insert(Iterable<S> entities);

	/**
	 * Inserts the given a given entities. Assumes the instance to be new to be able to apply insertion optimizations. Use
	 * the returned instance for further operations as the save operation might have changed the entity instance
	 * completely. Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Flux<S> insert(Publisher<S> entities);

	/**
	 * {@inheritDoc}
	 * <p>
	 * Note: Cassandra supports single-field {@code IN} queries only. Fetches each row individually when using
	 * {@link org.springframework.data.cassandra.core.mapping.MapId} with multiple components.
	 */
	@Override
	Flux<T> findAllById(Iterable<ID> iterable);

	/**
	 * {@inheritDoc}
	 * <p>
	 * Fetches each row individually.
	 */
	@Override
	Flux<T> findAllById(Publisher<ID> publisher);
}
