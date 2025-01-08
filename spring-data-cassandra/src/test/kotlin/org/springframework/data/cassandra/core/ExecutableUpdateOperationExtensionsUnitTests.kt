/*
 * Copyright 2018-2025 the original author or authors.
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

/**
 * Unit tests for [ExecutableUpdateOperationExtensions].
 *
 * @author Mark Paluch
 */
class ExecutableUpdateOperationExtensionsUnitTests {

	val operations = mockk<FluentCassandraOperations>(relaxed = true)

	@Test // DATACASS-484
	fun `update(KClass) extension should call its Java counterpart`() {

		operations.update(Person::class);
		verify { operations.update(Person::class.java) }
	}

	@Test // DATACASS-484
	fun `update() with reified type parameter extension should call its Java counterpart`() {

		operations.update<Person>()
		verify { operations.update(Person::class.java) }
	}
}
