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
import org.springframework.data.domain.Slice
import org.springframework.util.concurrent.ListenableFuture
import kotlin.reflect.KClass

/**
 * Extensions for [AsyncCassandraOperations].
 *
 * @author Mark Paluch
 * @since 2.1
 */

// -------------------------------------------------------------------------
// Methods dealing with static CQL
// -------------------------------------------------------------------------

/**
 * Extension for [AsyncCassandraOperations.select] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(cql)"))
fun <T : Any> AsyncCassandraOperations.select(cql: String, entityClass: KClass<T>): ListenableFuture<List<T>> =
		select(cql, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.select(cql: String): ListenableFuture<List<T>> =
		select(cql, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.select] providing a Consumer-like function.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(cql, consumer)"))
fun <T : Any> AsyncCassandraOperations.select(cql: String, entityClass: KClass<T>, consumer: (T) -> Unit): ListenableFuture<Void> =
		select(cql, consumer, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.select] providing a Consumer-like function leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.select(cql: String, crossinline consumer: (T) -> Unit): ListenableFuture<Void> =
		select(cql, { consumer(it) }, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.selectOne] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("selectOne<T>(cql)"))
fun <T : Any> AsyncCassandraOperations.selectOne(cql: String, entityClass: KClass<T>): ListenableFuture<T?> =
		selectOne(cql, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.selectOne(cql: String): ListenableFuture<T?> =
		selectOne(cql, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
// -------------------------------------------------------------------------

/**
 * Extension for [AsyncCassandraOperations.select] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(statement)"))
fun <T : Any> AsyncCassandraOperations.select(statement: Statement<*>, entityClass: KClass<T>): ListenableFuture<List<T>> =
		select(statement, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.select(statement: Statement<*>): ListenableFuture<List<T>> =
		select(statement, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.select] providing a Consumer-like function.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(statement, consumer)"))
fun <T : Any> AsyncCassandraOperations.select(statement: Statement<*>, entityClass: KClass<T>, consumer: (T) -> Unit): ListenableFuture<Void> =
		select(statement, consumer, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.select] providing a Consumer-like function leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.select(statement: Statement<*>, crossinline consumer: (T) -> Unit): ListenableFuture<Void> =
		select(statement, { consumer(it) }, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.slice] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("slice<T>(statement)"))
fun <T : Any> AsyncCassandraOperations.slice(statement: Statement<*>, entityClass: KClass<T>): ListenableFuture<Slice<T>> =
		slice(statement, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.slice] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.slice(statement: Statement<*>): ListenableFuture<Slice<T>> =
		slice(statement, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.selectOne] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("selectOne<T>(statement)"))
fun <T : Any> AsyncCassandraOperations.selectOne(statement: Statement<*>, entityClass: KClass<T>): ListenableFuture<T?> =
		selectOne(statement, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.selectOne(statement: Statement<*>): ListenableFuture<T?> =
		selectOne(statement, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with org.springframework.data.cassandra.core.query.Query
// -------------------------------------------------------------------------

/**
 * Extension for [AsyncCassandraOperations.select] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(query)"))
fun <T : Any> AsyncCassandraOperations.select(query: Query, entityClass: KClass<T>): ListenableFuture<List<T>> =
		select(query, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.select(query: Query): ListenableFuture<List<T>> =
		select(query, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.select] providing a Consumer-like function.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(query, consumer)"))
fun <T : Any> AsyncCassandraOperations.select(query: Query, entityClass: KClass<T>, consumer: (T) -> Unit): ListenableFuture<Void> =
		select(query, consumer, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.select] providing a Consumer-like function leveraging reified type parameters.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("select<T>(query, consumer)"))
inline fun <reified T : Any> AsyncCassandraOperations.select(query: Query, crossinline consumer: (T) -> Unit): ListenableFuture<Void> =
		select(query, { consumer(it) }, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.slice] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("slice<T>(query)"))
fun <T : Any> AsyncCassandraOperations.slice(query: Query, entityClass: KClass<T>): ListenableFuture<Slice<T>> =
		slice(query, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.slice] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.slice(query: Query): ListenableFuture<Slice<T>> =
		slice(query, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.selectOne] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("selectOne<T>(query)"))
fun <T : Any> AsyncCassandraOperations.selectOne(query: Query, entityClass: KClass<T>): ListenableFuture<T?> =
		selectOne(query, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.selectOne] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.selectOne(query: Query): ListenableFuture<T?> =
		selectOne(query, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.update] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("update<T>(query, update)"))
fun <T : Any> AsyncCassandraOperations.update(query: Query, update: Update, entityClass: KClass<T>): ListenableFuture<Boolean> =
		update(query, update, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.update] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.update(query: Query, update: Update): ListenableFuture<Boolean> =
		update(query, update, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.delete] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("delete<T>(query)"))
fun <T : Any> AsyncCassandraOperations.delete(query: Query, entityClass: KClass<T>): ListenableFuture<Boolean> =
		delete(query, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.delete] leveraging reified type parameters.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> AsyncCassandraOperations.delete(query: Query): ListenableFuture<Boolean> =
		delete(query, T::class.java)

// -------------------------------------------------------------------------
// Methods dealing with entities
// -------------------------------------------------------------------------

/**
 * Extension for [AsyncCassandraOperations.count] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("count<T>()"))
fun <T : Any> AsyncCassandraOperations.count(entityClass: KClass<T>): ListenableFuture<Long> =
		count(entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.count(): ListenableFuture<Long> =
		count(T::class.java)

/**
 * Extension for [AsyncCassandraOperations.count] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("count<T>(query)"))
fun <T : Any> AsyncCassandraOperations.count(query: Query, entityClass: KClass<T>): ListenableFuture<Long> =
		count(query, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.count(query: Query): ListenableFuture<Long> =
		count(query, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.exists] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("exists<T>(id)"))
fun <T : Any> AsyncCassandraOperations.exists(id: Any, entityClass: KClass<T>): ListenableFuture<Boolean> =
		exists(id, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.exists] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.exists(id: Any): ListenableFuture<Boolean> =
		exists(id, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.count] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("exists<T>(query)"))
fun <T : Any> AsyncCassandraOperations.exists(query: Query, entityClass: KClass<T>): ListenableFuture<Boolean> =
		exists(query, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.count] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.exists(query: Query): ListenableFuture<Boolean> =
		exists(query, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.selectOneById] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("selectOneById<T>(id)"))
fun <T : Any> AsyncCassandraOperations.selectOneById(id: Any, entityClass: KClass<T>): ListenableFuture<T?> =
		selectOneById(id, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.selectOneById] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.selectOneById(id: Any): ListenableFuture<T?> =
		selectOneById(id, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.deleteById] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("deleteById<T>(id)"))
fun <T : Any> AsyncCassandraOperations.deleteById(id: Any, entityClass: KClass<T>): ListenableFuture<Boolean> =
		deleteById(id, entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.deleteById] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.deleteById(id: Any): ListenableFuture<Boolean> =
		deleteById(id, T::class.java)

/**
 * Extension for [AsyncCassandraOperations.truncate] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("truncate<T>()"))
fun <T : Any> AsyncCassandraOperations.truncate(entityClass: KClass<T>): ListenableFuture<Void> =
		truncate(entityClass.java)

/**
 * Extension for [AsyncCassandraOperations.truncate] leveraging reified type parameters.
 */
inline fun <reified T : Any> AsyncCassandraOperations.truncate(): ListenableFuture<Void> =
		truncate(T::class.java)
