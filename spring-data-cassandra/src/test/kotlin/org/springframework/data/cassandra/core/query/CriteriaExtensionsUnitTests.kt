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
package org.springframework.data.cassandra.core.query

import com.nhaarman.mockito_kotlin.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Answers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.MockitoJUnitRunner

/**
 * Unit tests for [CriteriaExtensions].
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner::class)
class CriteriaExtensionsUnitTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var criteria: Criteria

	@Test
	fun `isEqualTo() extension should call its Java counterpart`() {

		val foo = "foo"
		criteria.isEqualTo(foo)

		verify(criteria).`is`(foo)
	}

	@Test
	fun `isEqualTo() extension should support nullable value`() {

		criteria.isEqualTo(null)

		verify(criteria).`is`(null)
	}

	@Test
	fun `inValues(varags) extension should call its Java counterpart`() {

		val foo = "foo"
		val bar = "bar"

		criteria.inValues(foo, bar)

		Mockito.verify(criteria).`in`(foo, bar)
	}

	@Test
	fun `inValues(varags) extension should support nullable values`() {

		criteria.inValues(null, null)

		Mockito.verify(criteria).`in`(null, null)
	}

	@Test
	fun `inValues(Collection) extension should call its Java counterpart`() {

		val c = listOf("foo", "bar")

		criteria.inValues(c)

		verify(criteria).`in`(c)
	}

	@Test
	fun `inValues(Collection) extension should support nullable values`() {

		val c = listOf("foo", null, "bar")

		criteria.inValues(c)

		verify(criteria).`in`(c)
	}

	@Test
	fun `where(String) should create Criteria`() {

		val criteria = where("foo").isEqualTo("bar");

		assertThat(criteria.columnName.toCql()).isEqualTo("foo")
		assertThat(criteria.predicate).isNotNull()
	}

	@Test
	fun `and(CriteriaDefinition) should concatenate criteria`() {

		val criteriaDefinitions = where("foo").isEqualTo("bar") and where("baz").isEqualTo("bar") and where("name").isEqualTo("Doe")

		assertThat(criteriaDefinitions.criteriaDefinitions).hasSize(3)
	}
}

