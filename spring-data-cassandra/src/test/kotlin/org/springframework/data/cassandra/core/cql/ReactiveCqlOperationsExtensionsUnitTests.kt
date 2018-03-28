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

import com.datastax.driver.core.Row
import com.datastax.driver.core.SimpleStatement
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.verify
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Answers
import org.mockito.Mock
import org.mockito.junit.MockitoJUnitRunner
import org.springframework.data.cassandra.domain.Person

/**
 * Unit tests for [ReactiveCqlOperationsExtensions].
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner::class)
class ReactiveCqlOperationsExtensionsUnitTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operations: ReactiveCqlOperations

	@Test // DATACASS-484
	fun `queryForObject(String, KClass) extension should call its Java counterpart`() {

		operations.queryForObject("", Person::class)
		verify(operations).queryForObject("", Person::class.java)
	}

	@Test // DATACASS-484
	fun `queryForObject(String) extension should call its Java counterpart`() {

		operations.queryForObject<Person>("")
		verify(operations).queryForObject("", Person::class.java)
	}

	@Test // DATACASS-484
	fun `queryForObject(String, KClass, array) extension should call its Java counterpart`() {

		operations.queryForObject("", Person::class, "foo", "bar")
		verify(operations).queryForObject("", Person::class.java, arrayOf("foo", "bar"))
	}

	@Test // DATACASS-484
	fun `queryForObject(String, array) extension should call its Java counterpart`() {

		operations.queryForObject<Person>("", "foo", "bar")
		verify(operations).queryForObject("", Person::class.java, arrayOf("foo", "bar"))
	}

	@Test // DATACASS-484
	fun `queryForObject(String, RowMapper, array) extension should call its Java counterpart`() {

		operations.queryForObject("", 3) { rs: Row, _: Int -> rs.getInt(1) }
		verify(operations).queryForObject(eq(""), any<RowMapper<Int>>(), eq(3))
	}

	@Test // DATACASS-484
	fun `queryForObject(Statement, KClass) extension should call its Java counterpart`() {

		val statement = SimpleStatement("SELECT * FROM person")

		operations.queryForObject(statement, Person::class)
		verify(operations).queryForObject(statement, Person::class.java)
	}

	@Test // DATACASS-484
	fun `queryForObject(Statement) extension should call its Java counterpart`() {

		val statement = SimpleStatement("SELECT * FROM person")

		operations.queryForObject<Person>(statement)
		verify(operations).queryForObject(statement, Person::class.java)
	}

	@Test // DATACASS-484
	fun `queryForFlux(String) extension should call its Java counterpart`() {

		operations.queryForFlux<Person>("")
		verify(operations).queryForFlux("", Person::class.java)
	}

	@Test // DATACASS-484
	fun `queryForFlux(String, array) extension should call its Java counterpart`() {

		operations.queryForFlux<Person>("", "foo", "bar")
		verify(operations).queryForFlux("", Person::class.java, arrayOf("foo", "bar"))
	}

	@Test // DATACASS-484
	@Ignore("No such method: queryForFlux(CQL, RowMapper, Args), see DATACASS-538")
	fun `queryForFlux(String, RowMapper, array) extension should call its Java counterpart`() {

		//operations.queryForFlux("", 3) { rs: Row, _: Int -> rs.getInt(1) }
		verify(operations).queryForFlux(eq(""), any<RowMapper<Int>>(), eq(3))
	}

	@Test // DATACASS-484
	fun `queryForFlux(Statement, KClass) extension should call its Java counterpart`() {

		val statement = SimpleStatement("SELECT * FROM person")

		operations.queryForFlux(statement, Person::class)
		verify(operations).queryForFlux(statement, Person::class.java)
	}

	@Test // DATACASS-484
	fun `queryForFlux(Statement) extension should call its Java counterpart`() {

		val statement = SimpleStatement("SELECT * FROM person")

		operations.queryForFlux<Person>(statement)
		verify(operations).queryForFlux(statement, Person::class.java)
	}

	@Test // DATACASS-484
	fun `query(String, ResultSetExtractor, array) extension should call its Java counterpart`() {

		operations.query("", 3) { rs -> rs.rows().next().cast(Person::class.java) }
		verify(operations).query(eq(""), any<ReactiveResultSetExtractor<Person>>(), eq(3))
	}

	@Test // DATACASS-484
	fun `query(String, RowMapper, array) extension should call its Java counterpart`() {

		operations.query("", 3) { row, _ -> row.columnDefinitions }
		verify(operations).query(eq(""), any<RowMapper<Person>>(), eq(3))
	}
}
