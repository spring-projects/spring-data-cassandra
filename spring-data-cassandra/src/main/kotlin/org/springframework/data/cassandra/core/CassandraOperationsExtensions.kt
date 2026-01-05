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

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql.Statement
import org.springframework.data.cassandra.core.query.Query
import org.springframework.data.cassandra.core.query.Update
import org.springframework.data.domain.Slice
import java.util.stream.Stream

/**
 * Extensions for [CassandraOperations].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [CassandraOperations.getTableName] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.getTableName(): CqlIdentifier =
		getTableName(T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with static CQL
// -------------------------------------------------------------------------

/**
 * Extension for [CassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.select(cql: String): List<T> =
		select(cql, T::class.java)

/**
 * Extension for [CassandraOperations.stream] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.stream(cql: String): Stream<T> =
		stream(cql, T::class.java)

/**
 * Extension for [CassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.selectOne(cql: String): T? =
		selectOne(cql, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
// -------------------------------------------------------------------------

/**
 * Extension for [CassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.select(statement: Statement<*>): List<T> =
		select(statement, T::class.java)

/**
 * Extension for [CassandraOperations.slice] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.slice(statement: Statement<*>): Slice<T> =
		slice(statement, T::class.java)

/**
 * Extension for [CassandraOperations.stream] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.stream(statement: Statement<*>): Stream<T> =
		stream(statement, T::class.java)

/**
 * Extension for [CassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.selectOne(statement: Statement<*>): T? =
		selectOne(statement, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with org.springframework.data.cassandra.core.query.Query
// -------------------------------------------------------------------------

/**
 * Extension for [CassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.select(query: Query): List<T> =
		select(query, T::class.java)

/**
 * Extension for [CassandraOperations.slice] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.slice(query: Query): Slice<T> =
		slice(query, T::class.java)

/**
 * Extension for [CassandraOperations.stream] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.stream(query: Query): Stream<T> =
		stream(query, T::class.java)

/**
 * Extension for [CassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.selectOne(query: Query): T? =
		selectOne(query, T::class.java)

/**
 * Extension for [CassandraOperations.update] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.update(query: Query, update: Update): Boolean =
		update(query, update, T::class.java)

/**
 * Extension for [CassandraOperations.delete] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> CassandraOperations.delete(query: Query): Boolean =
		delete(query, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with entities
// -------------------------------------------------------------------------

/**
 * Extension for [CassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.count(): Long =
		count(T::class.java)

/**
 * Extension for [CassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.count(query: Query): Long =
		count(query, T::class.java)

/**
 * Extension for [CassandraOperations.exists] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.exists(id: Any): Boolean =
		exists(id, T::class.java)

/**
 * Extension for [CassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.exists(query: Query): Boolean =
		exists(query, T::class.java)

/**
 * Extension for [CassandraOperations.selectOneById] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.selectOneById(id: Any): T? =
		selectOneById(id, T::class.java)

/**
 * Extension for [CassandraOperations.deleteById] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.deleteById(id: Any): Boolean =
		deleteById(id, T::class.java)

/**
 * Extension for [CassandraOperations.truncate] leveraging reified type parameters.
 */
inline fun <reified T : Any> CassandraOperations.truncate(): Unit =
		truncate(T::class.java)
