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

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.data.cassandra.core.query.Update
import org.springframework.data.cassandra.domain.Person
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveUpdateOperationExtensions].
 *
 * @author Mark Paluch
 */
class ReactiveUpdateOperationExtensionsUnitTests {

	val operations = mockk<ReactiveFluentCassandraOperations>(relaxed = true)

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

	@Test // DATACASS-632
	fun applyAndAwait() {

		val update = mockk<ReactiveUpdateOperation.TerminatingUpdate>()
		val result = mockk<WriteResult>()
		val updateObj = mockk<Update>();
		every { update.apply(updateObj) } returns Mono.just(result)

		runBlocking {
			Assertions.assertThat(update.applyAndAwait(updateObj)).isEqualTo(result)
		}

		verify {
			update.apply(updateObj)
		}
	}
}
