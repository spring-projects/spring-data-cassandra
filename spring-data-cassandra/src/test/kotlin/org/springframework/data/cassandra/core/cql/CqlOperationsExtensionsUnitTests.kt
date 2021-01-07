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
package org.springframework.data.cassandra.core.cql

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.data.cassandra.domain.Person

/**
 * Unit tests for [CqlOperationsExtensions].
 *
 * @author Mark Paluch
 */
class CqlOperationsExtensionsUnitTests {

	val operations = mockk<CqlOperations>(relaxed = true)

	@Test // DATACASS-484
	fun `queryForObject(String, KClass) extension should call its Java counterpart`() {

		operations.queryForObject("", Person::class)
		verify { operations.queryForObject("", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `queryForObject(String) extension should call its Java counterpart`() {

		operations.queryForObject<Person>("")
		verify { operations.queryForObject("", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `queryForObject(String, KClass, array) extension should call its Java counterpart`() {

		operations.queryForObject("", Person::class, "foo", "bar")
		verify { operations.queryForObject("", Person::class.java, arrayOf("foo", "bar")) }
	}

	@Test // DATACASS-484
	fun `queryForObject(String, array) extension should call its Java counterpart`() {

		operations.queryForObject<Person>("", "foo", "bar")
		verify { operations.queryForObject("", Person::class.java, arrayOf("foo", "bar")) }
	}

	@Test // DATACASS-484
	fun `queryForObject(String, RowMapper, array) extension should call its Java counterpart`() {

		operations.queryForObject("", 3) { rs: Row, _: Int -> rs.getInt(1) }
		verify { operations.queryForObject(eq(""), any<RowMapper<Int>>(), eq(3)) }
	}

	@Test // DATACASS-484
	fun `queryForObject(Statement, KClass) extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")

		operations.queryForObject(statement, Person::class)
		verify { operations.queryForObject(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `queryForObject(Statement) extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")

		operations.queryForObject<Person>(statement)
		verify { operations.queryForObject(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `queryForList(String) extension should call its Java counterpart`() {

		operations.queryForList<Person>("")
		verify { operations.queryForList("", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `queryForList(String, array) extension should call its Java counterpart`() {

		operations.queryForList<Person>("", "foo", "bar")
		verify { operations.queryForList("", Person::class.java, arrayOf("foo", "bar")) }
	}

	@Test // DATACASS-484
	fun `queryForList(Statement, KClass) extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")

		operations.queryForList(statement, Person::class)
		verify { operations.queryForList(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `queryForList(Statement) extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")

		operations.queryForList<Person>(statement)
		verify { operations.queryForList(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `query(String, ResultSetExtractor, array) extension should call its Java counterpart`() {

		operations.query<Person>("", 3) { rs -> Person("Walter", rs.single().getString(0)) }
		verify { operations.query(eq(""), any<ResultSetExtractor<Person>>(), eq(3)) }
	}

	@Test // DATACASS-484
	fun `query(String, RowCallbackHandler, array) extension should call its Java counterpart`() {

		operations.query("", 3) { row -> row.columnDefinitions }
		verify { operations.query(eq(""), any(), eq(3)) }
	}

	@Test // DATACASS-484
	fun `query(String, RowMapper, array) extension should call its Java counterpart`() {

		operations.query("", 3) { row, _ -> row.columnDefinitions }
		verify { operations.query(eq(""), any<RowMapper<Person>>(), eq(3)) }
	}

	@Test // DATACASS-809
	fun `queryForStream(String, RowMapper, array) extension should call its Java counterpart`() {

		operations.queryForStream("", 3) { row, _ -> row.columnDefinitions }
		verify { operations.queryForStream(eq(""), any<RowMapper<Person>>(), eq(3)) }
	}
}
