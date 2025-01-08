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
package org.springframework.data.cassandra.core

import kotlin.reflect.KClass

/**
 * Extensions for [ExecutableSelectOperation].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [ExecutableSelectOperation.query] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("query<T>()"))
fun <T : Any> ExecutableSelectOperation.query(entityClass: KClass<T>): ExecutableSelectOperation.ExecutableSelect<T> =
		query(entityClass.java)

/**
 * Extension for [ExecutableSelectOperation.query] leveraging reified type parameters.
 */
inline fun <reified T : Any> ExecutableSelectOperation.query(): ExecutableSelectOperation.ExecutableSelect<T> =
		query(T::class.java)

/**
 * Extension for [ExecutableSelectOperation.SelectWithProjection. as] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("asType<T>()"))
fun <T : Any> ExecutableSelectOperation.SelectWithProjection<*>.asType(resultType: KClass<T>): ExecutableSelectOperation.SelectWithQuery<T> =
		`as`(resultType.java)

/**
 * Extension for [ExecutableSelectOperation.FindWithProjection. as] leveraging reified type parameters.
 */
inline fun <reified T : Any> ExecutableSelectOperation.SelectWithProjection<*>.asType(): ExecutableSelectOperation.SelectWithQuery<T> =
		`as`(T::class.java)
