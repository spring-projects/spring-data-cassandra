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
package org.springframework.data.cassandra.core.query

/**
 * Extensions for [Criteria].
 *
 * @author Mark Paluch
 * @since 2.1
 */

/**
 * Extension for [Criteria. is] providing an `isEqualTo` alias since `in` is a reserved keyword in Kotlin.
 */
fun Criteria.isEqualTo(o: Any?): CriteriaDefinition = `is`(o)

/**
 * Extension for [Criteria. in] providing an `inValues` alias since `in` is a reserved keyword in Kotlin.
 */
fun <T : Any?> Criteria.inValues(c: Collection<T>): CriteriaDefinition = `in`(c)

/**
 * Extension for [Criteria. in] providing an `inValues` alias since `in` is a reserved keyword in Kotlin.
 */
fun Criteria.inValues(vararg o: Any?): CriteriaDefinition = `in`(*o)

/**
 * Extension for [CriteriaDefinition] to concatenate [CriteriaDefinition] using infix and.
 */
infix fun CriteriaDefinition.and(criteriaDefinition: CriteriaDefinition): CriteriaDefinitions {
	return CriteriaDefinitions(listOf(this, criteriaDefinition))
}

/**
 * Extension for [where] providing a global where alias for an improved DSL.
 */
fun where(columnName: String): Criteria = Criteria.where(columnName)

/**
 * Extension for [CriteriaDefinitions] adding and infix operator to [CriteriaDefinitions].
 */
infix fun CriteriaDefinitions.and(criteriaDefinition: CriteriaDefinition): CriteriaDefinitions {
	return CriteriaDefinitions(this.criteriaDefinitions + listOf(criteriaDefinition))
}

/**
 * Utility class to hold multiple [CriteriaDefinition].
 */
class CriteriaDefinitions(internal val criteriaDefinitions: List<CriteriaDefinition>)
