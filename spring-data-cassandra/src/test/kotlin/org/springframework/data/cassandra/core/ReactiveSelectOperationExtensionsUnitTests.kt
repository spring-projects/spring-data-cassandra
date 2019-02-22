/*
 * Copyright 2018-2019 the original author or authors.
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

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions
import org.junit.Test
import org.springframework.data.cassandra.domain.Person
import org.springframework.data.cassandra.domain.User
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveSelectOperationExtensions].
 *
 * @author Mark Paluch
 */
class ReactiveSelectOperationExtensionsUnitTests {

	val operations = mockk<ReactiveCassandraOperations>(relaxed = true)

	val operationWithProjection = mockk<ReactiveSelectOperation.SelectWithProjection<User>>(relaxed = true)

	@Test // DATACASS-484
	fun `query(KClass) extension should call its Java counterpart`() {

		operations.query(Person::class);
		verify { operations.query(Person::class.java) }
	}

	@Test // DATACASS-484
	fun `query() with reified type parameter extension should call its Java counterpart`() {

		operations.query<Person>()
		verify { operations.query(Person::class.java) }
	}

	@Test // DATACASS-484
	fun `asType(KClass) extension should call its Java counterpart`() {

		operationWithProjection.asType(User::class);
		verify { operationWithProjection.`as`(User::class.java) }
	}

	@Test // DATACASS-484
	fun `asType() with reified type parameter extension should call its Java counterpart`() {

		operationWithProjection.asType<User>();
		verify { operationWithProjection.`as`(User::class.java) }
	}

	@Test // DATACASS-632
	fun terminatingFindAwaitOne() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.one() } returns Mono.just("foo")

		runBlocking {
			Assertions.assertThat(find.awaitOne()).isEqualTo("foo")
		}

		verify {
			find.one()
		}
	}

	@Test // DATACASS-632
	fun terminatingFindAwaitFirst() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.first() } returns Mono.just("foo")

		runBlocking {
			Assertions.assertThat(find.awaitFirst()).isEqualTo("foo")
		}

		verify {
			find.first()
		}
	}

	@Test // DATACASS-632
	fun terminatingFindAwaitCount() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.count() } returns Mono.just(1)

		runBlocking {
			Assertions.assertThat(find.awaitCount()).isEqualTo(1)
		}

		verify {
			find.count()
		}
	}

	@Test // DATACASS-632
	fun terminatingFindAwaitExists() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.exists() } returns Mono.just(true)

		runBlocking {
			Assertions.assertThat(find.awaitExists()).isTrue()
		}

		verify {
			find.exists()
		}
	}
}
