/*
 * Copyright 2018-present the original author or authors.
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
package org.springframework.data.cassandra.core

import com.datastax.oss.driver.api.core.cql.Statement
import org.springframework.data.cassandra.core.query.Query
import org.springframework.data.cassandra.core.query.Update
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Extensions for [ReactiveCassandraOperations].
 *
 * @author Mark Paluch
 * @since 2.1
 */

// -------------------------------------------------------------------------
// Methods dealing with static CQL
// -------------------------------------------------------------------------

/**
 * Extension for [ReactiveCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.select(cql: String): Flux<T> =
		select(cql, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.selectOne(cql: String): Mono<T> =
		selectOne(cql, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
// -------------------------------------------------------------------------

/**
 * Extension for [ReactiveCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.select(statement: Statement<*>): Flux<T> =
		select(statement, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.selectOne(statement: Statement<*>): Mono<T> =
		selectOne(statement, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with org.springframework.data.cassandra.core.query.Query
// -------------------------------------------------------------------------

/**
 * Extension for [ReactiveCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.select(query: Query): Flux<T> =
		select(query, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.selectOne(query: Query): Mono<T> =
		selectOne(query, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.update] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.update(query: Query, update: Update): Mono<Boolean> =
		update(query, update, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.delete] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveCassandraOperations.delete(query: Query): Mono<Boolean> =
		delete(query, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with entities
// -------------------------------------------------------------------------

/**
 * Extension for [ReactiveCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.count(): Mono<Long> =
		count(T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.count(query: Query): Mono<Long> =
		count(query, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.exists] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.exists(id: Any): Mono<Boolean> =
		exists(id, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.exists(query: Query): Mono<Boolean> =
		exists(query, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOneById] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.selectOneById(id: Any): Mono<T> =
		selectOneById(id, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.deleteById] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.deleteById(id: Any): Mono<Boolean> =
		deleteById(id, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.truncate] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.truncate(): Mono<Void> =
		truncate(T::class.java)
