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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlin.reflect.KClass

/**
 * Extensions for [ReactiveSelectOperation].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [ReactiveSelectOperation.query] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("query<T>()"))
fun <T : Any> ReactiveSelectOperation.query(entityClass: KClass<T>): ReactiveSelectOperation.ReactiveSelect<T> =
		query(entityClass.java)

/**
 * Extension for [ReactiveSelectOperation.query] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveSelectOperation.query(): ReactiveSelectOperation.ReactiveSelect<T> =
		query(T::class.java)

/**
 * Extension for [ReactiveSelectOperation.SelectWithProjection. as] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("asType<T>()"))
fun <T : Any> ReactiveSelectOperation.SelectWithProjection<*>.asType(resultType: KClass<T>): ReactiveSelectOperation.SelectWithQuery<T> =
		`as`(resultType.java)

/**
 * Extension for [ReactiveSelectOperation.FindWithProjection. as] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveSelectOperation.SelectWithProjection<*>.asType(): ReactiveSelectOperation.SelectWithQuery<T> =
		`as`(T::class.java)

/**
 * Non-nullable Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.one].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend inline fun <reified T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitOne(): T =
		one().awaitSingle()

/**
 * Nullable Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.one].
 *
 * @author Mark Paluch
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend inline fun <reified T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitOneOrNull(): T? =
		one().awaitFirstOrNull()

/**
 * Non-nullable Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.first].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend inline fun <reified T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitFirst(): T =
		first().awaitSingle()

/**
 * Nullable Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.first].
 *
 * @author Mark Paluch
 * @author Sebastien Deleuze
 * @since 2.2
 */
suspend inline fun <reified T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitFirstOrNull(): T? =
		first().awaitFirstOrNull()

/**
 * Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.count].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitCount(): Long =
		count().awaitSingle()

/**
 * Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.exists].
 *
 * @author Mark Paluch
 * @since 2.2
 */
suspend fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitExists(): Boolean =
		exists().awaitSingle()

/**
 * Coroutines [Flow] variant of [ReactiveSelectOperation.TerminatingSelect.all].
 *
 * @author Sebastien Deleuze
 * @since 2.2
 */
fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.flow(): Flow<T> =
		all().asFlow()
