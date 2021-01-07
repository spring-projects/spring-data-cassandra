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

import kotlin.reflect.KClass

/**
 * Extensions for [ExecutableDeleteOperation].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [ExecutableRemoveOperation.delete] providing a [KClass] based variant.
 */
@Deprecated("Since 2.2, use the reified variant", replaceWith = ReplaceWith("delete<T>()"))
fun <T : Any> ExecutableDeleteOperation.delete(entityClass: KClass<T>): ExecutableDeleteOperation.ExecutableDelete =
		delete(entityClass.java)

/**
 * Extension for [ExecutableRemoveOperation.delete] leveraging reified type parameters.
 */
inline fun <reified T : Any> ExecutableDeleteOperation.delete(): ExecutableDeleteOperation.ExecutableDelete =
		delete(T::class.java)
