/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import kotlin.reflect.KClass

/**
 * Extensions for [CqlOperations].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [CqlOperations.queryForObject] providing a [KClass] based variant.
 */
fun <T : Any> CqlOperations.queryForObject(cql: String, entityClass: KClass<T>): T? =
		queryForObject(cql, entityClass.java)

/**
 * Extension for [CqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> CqlOperations.queryForObject(cql: String): T? =
		queryForObject(cql, T::class.java)

/**
 * Extension for [CqlOperations.queryForObject] providing a [KClass] based variant.
 */
fun <T : Any> CqlOperations.queryForObject(cql: String, entityClass: KClass<T>, vararg args: Any): T? =
		queryForObject(cql, entityClass.java, args)

/**
 * Extension for [CqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> CqlOperations.queryForObject(cql: String, vararg args: Any): T? =
		queryForObject(cql, T::class.java, args)

/**
 * Extension for [CqlOperations.queryForObject] leveraging reified type parameters.
 */
fun <T : Any> CqlOperations.queryForObject(cql: String, vararg args: Any, function: (Row, Int) -> T): T? =
		queryForObject(cql, function, args)

/**
 * Extension for [CqlOperations.queryForObject] providing a [KClass] based variant.
 */
fun <T : Any> CqlOperations.queryForObject(statement: Statement, entityClass: KClass<T>): T? =
		queryForObject(statement, entityClass.java)

/**
 * Extension for [CqlOperations.queryForObject] leveraging reified type parameters.
 */
inline fun <reified T : Any> CqlOperations.queryForObject(statement: Statement): T? =
		queryForObject(statement, T::class.java)

/**
 * Extension for [CqlOperations.queryForList] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> CqlOperations.queryForList(cql: String): List<T> =
		queryForList(cql, T::class.java)

/**
 * Extension for [CqlOperations.queryForList] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> CqlOperations.queryForList(cql: String, vararg args: Any): List<T> =
		queryForList(cql, T::class.java, args)

/**
 * Extension for [CqlOperations.queryForList] providing a [KClass] based variant.
 */
fun <T : Any> CqlOperations.queryForList(statement: Statement, entityClass: KClass<T>): List<T> =
		queryForList(statement, entityClass.java)

/**
 * Extension for [CqlOperations.queryForList] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> CqlOperations.queryForList(statement: Statement): List<T> =
		queryForList(statement, T::class.java)

/**
 * Extension for [CqlOperations.query] providing a ResultSetExtractor-like function
 * variant: `query("...", arg1, argN){ rs -> }`.
 */
inline fun <reified T : Any> CqlOperations.query(cql: String, vararg args: Any, crossinline function: (ResultSet) -> T): T? =
		query(cql, ResultSetExtractor { function(it) }, *args)

/**
 * Extension for [CqlOperations.query] providing a RowCallbackHandler-like function
 * variant: `query("...", arg1, argN){ rs -> }`.
 */
fun CqlOperations.query(cql: String, vararg args: Any, function: (Row) -> Unit): Unit =
		query(cql, RowCallbackHandler { function(it) }, *args)

/**
 * Extension for [CqlOperations.query] providing a RowMapper-like function
 * variant: `query("...", arg1, argN){ row, i -> }`.
 */
fun <T : Any> CqlOperations.query(cql: String, vararg args: Any, function: (Row, Int) -> T): List<T> =
		query(cql, RowMapper { row, i -> function(row, i) }, *args)
