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
package org.springframework.data.cassandra.core

import kotlin.reflect.KClass

/**
 * Extensions for [ReactiveUpdateOperation].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [ReactiveUpdateOperation.update] providing a [KClass] based variant.
 */
fun <T : Any> ReactiveUpdateOperation.update(entityClass: KClass<T>): ReactiveUpdateOperation.ReactiveUpdate =
		update(entityClass.java)

/**
 * Extension for [ReactiveUpdateOperation.update] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveUpdateOperation.update(): ReactiveUpdateOperation.ReactiveUpdate =
		update(T::class.java)
