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

import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.data.cassandra.domain.Person
import org.springframework.data.cassandra.domain.User

/**
 * Unit tests for [ExecutableSelectOperationExtensions].
 *
 * @author Mark Paluch
 */
class ExecutableSelectOperationExtensionsUnitTests {

	val operations = mockk<FluentCassandraOperations>(relaxed = true)

	val operationWithProjection = mockk<ExecutableSelectOperation.SelectWithProjection<User>>(relaxed = true)


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
}
