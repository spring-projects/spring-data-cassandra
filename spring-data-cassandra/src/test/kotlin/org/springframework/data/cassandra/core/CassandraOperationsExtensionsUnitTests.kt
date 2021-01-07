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

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.data.cassandra.core.query.Query
import org.springframework.data.cassandra.core.query.Update
import org.springframework.data.cassandra.domain.Person
import org.springframework.data.domain.SliceImpl

/**
 * Unit tests for [CassandraOperationsExtensions].
 *
 * @author Mark Paluch
 */
class CassandraOperationsExtensionsUnitTests {

	val operations = mockk<CassandraOperations>(relaxed = true)

	@Test // DATACASS-484
	fun `getTableName(KClass) extension should call its Java counterpart`() {

		every { operations.getTableName(Person::class.java) } returns CqlIdentifier.fromCql("person")

		operations.getTableName(Person::class)
		verify { operations.getTableName(Person::class.java) }
	}

	@Test // DATACASS-484
	fun `getTableName() with reified type parameter extension should call its Java counterpart`() {

		every { operations.getTableName(Person::class.java) } returns CqlIdentifier.fromCql("person")

		operations.getTableName<Person>()
		verify { operations.getTableName(Person::class.java) }
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static CQL
	// -------------------------------------------------------------------------

	@Test // DATACASS-484
	fun `select(String, KClass) extension should call its Java counterpart`() {

		operations.select("SELECT * FROM person", Person::class)
		verify { operations.select("SELECT * FROM person", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `select(String) with reified type parameter extension should call its Java counterpart`() {

		operations.select<Person>("SELECT * FROM person")
		verify { operations.select("SELECT * FROM person", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `stream(String, KClass) extension should call its Java counterpart`() {

		operations.stream("SELECT * FROM person", Person::class)
		verify { operations.stream("SELECT * FROM person", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `stream(String) with reified type parameter extension should call its Java counterpart`() {

		operations.stream<Person>("SELECT * FROM person")
		verify { operations.stream("SELECT * FROM person", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `selectOne(String, KClass) extension should call its Java counterpart`() {

		every { operations.selectOne("SELECT * FROM person", Person::class.java) } returns Person("Walter", "White")

		operations.selectOne("SELECT * FROM person", Person::class)
		verify { operations.selectOne("SELECT * FROM person", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `selectOne(String) with reified type parameter extension should call its Java counterpart`() {

		every { operations.selectOne("SELECT * FROM person", Person::class.java) } returns Person("Walter", "White")

		operations.selectOne<Person>("SELECT * FROM person")
		verify { operations.selectOne("SELECT * FROM person", Person::class.java) }
	}

	// -------------------------------------------------------------------------
	// Methods dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	@Test // DATACASS-484
	fun `select(Statement, KClass) extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")

		operations.select(statement, Person::class)
		verify { operations.select(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `select(Statement) with reified type parameter extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")
		operations.select<Person>(statement)
		verify { operations.select(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `slice(Statement, KClass) extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")
		every { operations.slice(statement, Person::class.java) } returns SliceImpl(listOf(Person("Walter", "White")))

		operations.slice(statement, Person::class)
		verify { operations.slice(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `slice(Statement) with reified type parameter extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")
		every { operations.slice(statement, Person::class.java) } returns SliceImpl(listOf(Person("Walter", "White")))

		operations.slice<Person>(statement)
		verify { operations.slice(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `stream(Statement, KClass) extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")

		operations.stream(statement, Person::class)
		verify { operations.stream(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `stream(Statement) with reified type parameter extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")

		operations.stream<Person>(statement)
		verify { operations.stream(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `selectOne(Statement, KClass) extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")
		every { operations.selectOne("SELECT * FROM person", Person::class.java) } returns Person("Walter", "White")

		operations.selectOne(statement, Person::class)
		verify { operations.selectOne(statement, Person::class.java) }
	}

	@Test // DATACASS-484
	fun `selectOne(Statement) with reified type parameter extension should call its Java counterpart`() {

		val statement = SimpleStatement.newInstance("SELECT * FROM person")
		every { operations.selectOne("SELECT * FROM person", Person::class.java) } returns Person("Walter", "White")

		operations.selectOne<Person>(statement)
		verify { operations.selectOne(statement, Person::class.java) }
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.cassandra.core.query.Query
	// -------------------------------------------------------------------------

	@Test // DATACASS-484
	fun `select(Query, KClass) extension should call its Java counterpart`() {

		operations.select(Query.empty(), Person::class)
		verify { operations.select(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `select(Query) with reified type parameter extension should call its Java counterpart`() {

		operations.select<Person>(Query.empty())
		verify { operations.select(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `slice(Query, KClass) extension should call its Java counterpart`() {

		every { operations.slice(Query.empty(), Person::class.java) } returns SliceImpl(listOf(Person("Walter", "White")))

		operations.slice(Query.empty(), Person::class)
		verify { operations.slice(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `slice(Query) with reified type parameter extension should call its Java counterpart`() {

		every { operations.slice(Query.empty(), Person::class.java) } returns SliceImpl(listOf(Person("Walter", "White")))

		operations.slice<Person>(Query.empty())
		verify { operations.slice(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `stream(Query, KClass) extension should call its Java counterpart`() {

		operations.stream(Query.empty(), Person::class)
		verify { operations.stream(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `stream(Query) with reified type parameter extension should call its Java counterpart`() {

		operations.stream<Person>(Query.empty())
		verify { operations.stream(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `selectOne(Query, KClass) extension should call its Java counterpart`() {

		every { operations.selectOne(Query.empty(), Person::class.java) } returns Person("Walter", "White")

		operations.selectOne(Query.empty(), Person::class)
		verify { operations.selectOne(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `selectOne(Query) with reified type parameter extension should call its Java counterpart`() {

		every { operations.selectOne(Query.empty(), Person::class.java) } returns Person("Walter", "White")

		operations.selectOne<Person>(Query.empty())
		verify { operations.selectOne(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `update(Query, Update, KClass) extension should call its Java counterpart`() {

		operations.update(Query.empty(), Update.empty(), Person::class)
		verify { operations.update(Query.empty(), Update.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `update(Query, Update) with reified type parameter extension should call its Java counterpart`() {

		operations.update<Person>(Query.empty(), Update.empty())
		verify { operations.update(Query.empty(), Update.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `delete(Query, KClass) extension should call its Java counterpart`() {

		operations.delete(Query.empty(), Person::class)
		verify { operations.delete(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `delete(Query,) with reified type parameter extension should call its Java counterpart`() {

		operations.delete<Person>(Query.empty())
		verify { operations.delete(Query.empty(), Person::class.java) }
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	@Test // DATACASS-484
	fun `count(KClass) extension should call its Java counterpart`() {

		operations.count(Person::class)
		verify { operations.count(Person::class.java) }
	}

	@Test // DATACASS-484
	fun `count() with reified type parameter extension should call its Java counterpart`() {

		operations.count<Person>()
		verify { operations.count(Person::class.java) }
	}

	@Test // DATACASS-484
	fun `count(Query, KClass) extension should call its Java counterpart`() {

		operations.count(Query.empty(), Person::class)
		verify { operations.count(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `count(Query) with reified type parameter extension should call its Java counterpart`() {

		operations.count<Person>(Query.empty())
		verify { operations.count(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `exists(Any, KClass) extension should call its Java counterpart`() {

		operations.exists("id", Person::class)
		verify { operations.exists("id", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `exists(Any) with reified type parameter extension should call its Java counterpart`() {

		operations.exists<Person>("id")
		verify { operations.exists("id", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `exists(Query, KClass) extension should call its Java counterpart`() {

		operations.exists(Query.empty(), Person::class)
		verify { operations.exists(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `exists(Query) with reified type parameter extension should call its Java counterpart`() {

		operations.exists<Person>(Query.empty())
		verify { operations.exists(Query.empty(), Person::class.java) }
	}

	@Test // DATACASS-484
	fun `selectOneById(Any, KClass) extension should call its Java counterpart`() {

		every { operations.selectOneById("id", Person::class.java) } returns Person("Walter", "White")

		operations.selectOneById("id", Person::class)
		verify { operations.selectOneById("id", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `selectOneById(Any) with reified type parameter extension should call its Java counterpart`() {

		every { operations.selectOneById("id", Person::class.java) } returns Person("Walter", "White")

		operations.selectOneById<Person>("id")
		verify { operations.selectOneById("id", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `deleteById(Any, KClass) extension should call its Java counterpart`() {

		operations.deleteById("id", Person::class)
		verify { operations.deleteById("id", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `deleteById(Any) with reified type parameter extension should call its Java counterpart`() {

		operations.deleteById<Person>("id")
		verify { operations.deleteById("id", Person::class.java) }
	}

	@Test // DATACASS-484
	fun `truncate(KClass) extension should call its Java counterpart`() {

		operations.truncate(Person::class)
		verify { operations.truncate(Person::class.java) }
	}

	@Test // DATACASS-484
	fun `truncate() with reified type parameter extension should call its Java counterpart`() {

		operations.truncate<Person>()
		verify { operations.truncate(Person::class.java) }
	}
}
