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
package org.springframework.data.cassandra.core.cql

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.Statement
import org.reactivestreams.Publisher
import org.springframework.data.cassandra.ReactiveResultSet
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.reflect.KClass

/**
 * Extensions for [ReactiveCqlOperations].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [ReactiveCqlOperations.queryForObject] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("queryForObject<T>(cql)"))
fun <T : Any> ReactiveCqlOperations.queryForObject(cql: String, entityClass: KClass<T>): Mono<T> =
		queryForObject(cql, entityClass.java)

/**
 * Extension for [ReactiveCqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCqlOperations.queryForObject(cql: String): Mono<T> =
		queryForObject(cql, T::class.java)

/**
 * Extension for [ReactiveCqlOperations.queryForObject] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("queryForObject<T>(cql, args)"))
fun <T : Any> ReactiveCqlOperations.queryForObject(cql: String, entityClass: KClass<T>, vararg args: Any): Mono<T> =
		queryForObject(cql, entityClass.java, args)

/**
 * Extension for [ReactiveCqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCqlOperations.queryForObject(cql: String, vararg args: Any): Mono<T> =
		queryForObject(cql, T::class.java, args)

/**
 * Extension for [ReactiveCqlOperations.queryForObject] leveraging reified type parameters.
 */
fun <T : Any> ReactiveCqlOperations.queryForObject(cql: String, vararg args: Any, function: (Row, Int) -> T): Mono<T> =
		queryForObject(cql, function, args)

/**
 * Extension for [ReactiveCqlOperations.queryForObject] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("queryForObject<T>(statement)"))
fun <T : Any> ReactiveCqlOperations.queryForObject(statement: Statement<*>, entityClass: KClass<T>): Mono<T> =
		queryForObject(statement, entityClass.java)

/**
 * Extension for [ReactiveCqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveCqlOperations.queryForObject(statement: Statement<*>): Mono<T> =
		queryForObject(statement, T::class.java)

/**
 * Extension for [ReactiveCqlOperations.queryForFlux] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveCqlOperations.queryForFlux(cql: String): Flux<T> =
		queryForFlux(cql, T::class.java)

/**
 * Extension for [ReactiveCqlOperations.queryForFlux] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveCqlOperations.queryForFlux(cql: String, vararg args: Any): Flux<T> =
		queryForFlux(cql, T::class.java, args)

/**
 * Extension for [ReactiveCqlOperations.queryForFlux] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("queryForFlux<T>(statement)"))
fun <T : Any> ReactiveCqlOperations.queryForFlux(statement: Statement<*>, entityClass: KClass<T>): Flux<T> =
		queryForFlux(statement, entityClass.java)

/**
 * Extension for [ReactiveCqlOperations.queryForFlux] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> ReactiveCqlOperations.queryForFlux(statement: Statement<*>): Flux<T> =
		queryForFlux(statement, T::class.java)

/**
 * Extension for [ReactiveCqlOperations.query] providing a ResultSetExtractor-like function
 * variant: `query("...", arg1, argN){ rs -> }`.
 */
inline fun <reified T : Any> ReactiveCqlOperations.query(cql: String, vararg args: Any, crossinline function: (ReactiveResultSet) -> Publisher<T>): Flux<T> =
		query(cql, ReactiveResultSetExtractor { function(it) }, *args)

/**
 * Extension for [ReactiveCqlOperations.query] providing a RowMapper-like function
 * variant: `query("...", arg1, argN){ row, i -> }`.
 */
fun <T : Any> ReactiveCqlOperations.query(cql: String, vararg args: Any, function: (Row, Int) -> T): Flux<T> =
		query(cql, RowMapper { row, i -> function(row, i) }, *args)
