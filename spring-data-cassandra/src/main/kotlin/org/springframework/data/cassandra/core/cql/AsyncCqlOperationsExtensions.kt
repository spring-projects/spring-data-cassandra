/*
 * Copyright 2018-2025 the original author or authors.
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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.Statement
import java.util.concurrent.CompletableFuture
import kotlin.reflect.KClass

/**
 * Extensions for [AsyncCqlOperations].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [AsyncCqlOperations.queryForObject] providing a [KClass] based variant.
 */
@Deprecated(
	"Since 2.2, use the reified variant",
	replaceWith = ReplaceWith("queryForObject<T>(cql)")
)
fun <T : Any> AsyncCqlOperations.queryForObject(
	cql: String,
	entityClass: KClass<T>
): CompletableFuture<T?> =
	queryForObject(cql, entityClass.java)

/**
 * Extension for [AsyncCqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCqlOperations.queryForObject(cql: String): CompletableFuture<T?> =
	queryForObject(cql, T::class.java)

/**
 * Extension for [AsyncCqlOperations.queryForObject] providing a [KClass] based variant.
 */
@Deprecated(
	"Since 2.2, use the reified variant",
	replaceWith = ReplaceWith("queryForObject<T>(cql, args)")
)
fun <T : Any> AsyncCqlOperations.queryForObject(
	cql: String,
	entityClass: KClass<T>,
	vararg args: Any
): CompletableFuture<T?> =
	queryForObject(cql, entityClass.java, *args)

/**
 * Extension for [AsyncCqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCqlOperations.queryForObject(
	cql: String,
	vararg args: Any
): CompletableFuture<T?> =
	queryForObject(cql, T::class.java, *args)

/**
 * Extension for [AsyncCqlOperations.queryForObject] leveraging reified type parameters.
 */
fun <T : Any> AsyncCqlOperations.queryForObject(
	cql: String,
	vararg args: Any,
	function: (Row, Int) -> T
): CompletableFuture<T?> =
	queryForObject(cql, { row, i -> function(row, i) }, *args)

/**
 * Extension for [AsyncCqlOperations.queryForObject] providing a [KClass] based variant.
 */
@Deprecated(
	"Since 2.2, use the reified variant",
	replaceWith = ReplaceWith("queryForObject<T>(statement)")
)
fun <T : Any> AsyncCqlOperations.queryForObject(
	statement: Statement<*>,
	entityClass: KClass<T>
): CompletableFuture<T?> =
	queryForObject(statement, entityClass.java)

/**
 * Extension for [AsyncCqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCqlOperations.queryForObject(statement: Statement<*>): CompletableFuture<T?> =
	queryForObject(statement, T::class.java)

/**
 * Extension for [AsyncCqlOperations.queryForList] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> AsyncCqlOperations.queryForList(cql: String): CompletableFuture<List<T>> =
	queryForList(cql, T::class.java)

/**
 * Extension for [AsyncCqlOperations.queryForList] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> AsyncCqlOperations.queryForList(
	cql: String,
	vararg args: Any
): CompletableFuture<List<T>> =
	queryForList(cql, T::class.java, *args)

/**
 * Extension for [AsyncCqlOperations.queryForList] providing a [KClass] based variant.
 */
@Deprecated(
	"Since 2.2, use the reified variant",
	replaceWith = ReplaceWith("queryForList<T>(statement)")
)
fun <T : Any> AsyncCqlOperations.queryForList(
	statement: Statement<*>,
	entityClass: KClass<T>
): CompletableFuture<List<T>> =
	queryForList(statement, entityClass.java)

/**
 * Extension for [AsyncCqlOperations.queryForList] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> AsyncCqlOperations.queryForList(statement: Statement<*>): CompletableFuture<List<T>> =
	queryForList(statement, T::class.java)

/**
 * Extension for [AsyncCqlOperations.query] providing a ResultSetExtractor-like function
 * variant: `query("...", arg1, argN){ rs -> }`.
 */
inline fun <reified T : Any> AsyncCqlOperations.query(
	cql: String,
	vararg args: Any,
	crossinline function: (AsyncResultSet) -> CompletableFuture<T>
): CompletableFuture<T?> =
	query(cql, AsyncResultSetExtractor { function(it) }, *args)

/**
 * Extension for [AsyncCqlOperations.query] providing a RowMapper-like function
 * variant: `query("...", arg1, argN){ row, i -> }`.
 */
fun <T : Any> AsyncCqlOperations.query(
	cql: String,
	vararg args: Any,
	function: (Row, Int) -> T
): CompletableFuture<List<T>> =
	query(cql, RowMapper { row, i -> function(row, i) }, *args)
