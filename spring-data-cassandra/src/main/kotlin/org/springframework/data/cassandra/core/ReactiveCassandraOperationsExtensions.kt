/*
 * Copyright 2018-2021 the original author or authors.
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
import kotlin.reflect.KClass

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
 * Extension for [ReactiveCassandraOperations.select] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(cql)"))
fun <T : Any> ReactiveCassandraOperations.select(cql: String, entityClass: KClass<T>): Flux<T> =
		select(cql, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.select(cql: String): Flux<T> =
		select(cql, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("selectOne<T>(cql)"))
fun <T : Any> ReactiveCassandraOperations.selectOne(cql: String, entityClass: KClass<T>): Mono<T> =
		selectOne(cql, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.selectOne(cql: String): Mono<T> =
		selectOne(cql, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
// -------------------------------------------------------------------------

/**
 * Extension for [ReactiveCassandraOperations.select] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(statement)"))
fun <T : Any> ReactiveCassandraOperations.select(statement: Statement<*>, entityClass: KClass<T>): Flux<T> =
		select(statement, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.select(statement: Statement<*>): Flux<T> =
		select(statement, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("selectOne<T>(statement)"))
fun <T : Any> ReactiveCassandraOperations.selectOne(statement: Statement<*>, entityClass: KClass<T>): Mono<T> =
		selectOne(statement, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.selectOne(statement: Statement<*>): Mono<T> =
		selectOne(statement, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with org.springframework.data.cassandra.core.query.Query
// -------------------------------------------------------------------------

/**
 * Extension for [ReactiveCassandraOperations.select] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(query)"))
fun <T : Any> ReactiveCassandraOperations.select(query: Query, entityClass: KClass<T>): Flux<T> =
		select(query, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.select(query: Query): Flux<T> =
		select(query, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("selectOne<T>(query)"))
fun <T : Any> ReactiveCassandraOperations.selectOne(query: Query, entityClass: KClass<T>): Mono<T> =
		selectOne(query, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.selectOne(query: Query): Mono<T> =
		selectOne(query, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.update] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("update<T>(query, update)"))
fun <T : Any> ReactiveCassandraOperations.update(query: Query, update: Update, entityClass: KClass<T>): Mono<Boolean> =
		update(query, update, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.update] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.update(query: Query, update: Update): Mono<Boolean> =
		update(query, update, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.delete] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("delete<T>(query)"))
fun <T : Any> ReactiveCassandraOperations.delete(query: Query, entityClass: KClass<T>): Mono<Boolean> =
		delete(query, entityClass.java)

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
 * Extension for [ReactiveCassandraOperations.count] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("count<T>()"))
fun <T : Any> ReactiveCassandraOperations.count(entityClass: KClass<T>): Mono<Long> =
		count(entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.count(): Mono<Long> =
		count(T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.count] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("count<T>(query)"))
fun <T : Any> ReactiveCassandraOperations.count(query: Query, entityClass: KClass<T>): Mono<Long> =
		count(query, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.count(query: Query): Mono<Long> =
		count(query, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.exists] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("exists<T>(id)"))
fun <T : Any> ReactiveCassandraOperations.exists(id: Any, entityClass: KClass<T>): Mono<Boolean> =
		exists(id, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.exists] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.exists(id: Any): Mono<Boolean> =
		exists(id, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.count] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("exists<T>(query)"))
fun <T : Any> ReactiveCassandraOperations.exists(query: Query, entityClass: KClass<T>): Mono<Boolean> =
		exists(query, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.exists(query: Query): Mono<Boolean> =
		exists(query, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOneById] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("selectOneById<T>(id)"))
fun <T : Any> ReactiveCassandraOperations.selectOneById(id: Any, entityClass: KClass<T>): Mono<T> =
		selectOneById(id, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.selectOneById] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.selectOneById(id: Any): Mono<T> =
		selectOneById(id, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.deleteById] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("deleteById<T>(id)"))
fun <T : Any> ReactiveCassandraOperations.deleteById(id: Any, entityClass: KClass<T>): Mono<Boolean> =
		deleteById(id, entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.deleteById] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.deleteById(id: Any): Mono<Boolean> =
		deleteById(id, T::class.java)

/**
 * Extension for [ReactiveCassandraOperations.truncate] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("truncate<T>()"))
fun <T : Any> ReactiveCassandraOperations.truncate(entityClass: KClass<T>): Mono<Void> =
		truncate(entityClass.java)

/**
 * Extension for [ReactiveCassandraOperations.truncate] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCassandraOperations.truncate(): Mono<Void> =
		truncate(T::class.java)
