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

import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit tests for [CriteriaExtensions].
 *
 * @author Mark Paluch
 */
class CriteriaExtensionsUnitTests {

	val criteria = mockk<Criteria>(relaxed = true)

	@Test // DATACASS-484
	fun `isEqualTo() extension should call its Java counterpart`() {

		val foo = "foo"
		criteria.isEqualTo(foo)

		verify { criteria.`is`(foo) }
	}

	@Test // DATACASS-484
	fun `isEqualTo() extension should support nullable value`() {

		criteria.isEqualTo(null)

		verify { criteria.`is`(null) }
	}

	@Test // DATACASS-484
	fun `inValues(varags) extension should call its Java counterpart`() {

		val foo = "foo"
		val bar = "bar"

		criteria.inValues(foo, bar)

		verify { criteria.`in`(foo, bar) }
	}

	@Test // DATACASS-484
	fun `inValues(varags) extension should support nullable values`() {

		criteria.inValues(null, null)

		verify { criteria.`in`(null, null) }
	}

	@Test // DATACASS-484
	fun `inValues(Collection) extension should call its Java counterpart`() {

		val c = listOf("foo", "bar")

		criteria.inValues(c)

		verify { criteria.`in`(c) }
	}

	@Test // DATACASS-484
	fun `inValues(Collection) extension should support nullable values`() {

		val c = listOf("foo", null, "bar")

		criteria.inValues(c)

		verify { criteria.`in`(c) }
	}

	@Test // DATACASS-484
	fun `where(String) should create Criteria`() {

		val criteria = where("foo").isEqualTo("bar");

		assertThat(criteria.columnName.toCql()).isEqualTo("foo")
		assertThat(criteria.predicate).isNotNull()
	}

	@Test // DATACASS-484
	fun `and(CriteriaDefinition) should concatenate criteria`() {

		val criteriaDefinitions = where("foo").isEqualTo("bar") and where("baz").isEqualTo("bar") and where("name").isEqualTo("Doe")

		assertThat(criteriaDefinitions.criteriaDefinitions).hasSize(3)
	}
}

