/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.query

import kotlinx.coroutines.flow.Flow
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext
import org.springframework.data.projection.SpelAwareProxyProjectionFactory
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import kotlin.coroutines.Continuation

/**
 * Unit tests for [ReactiveCassandraQueryMethod] using Coroutine repositories.
 *
 * @author Mark Paluch
 */
class ReactiveCassandraQueryMethodCoroutineUnitTests {

	val projectionFactory = SpelAwareProxyProjectionFactory()

	data class Person(val id: String)

	interface PersonRepository : CoroutineCrudRepository<Person, String> {

		suspend fun findSuspendAllByName(): Flow<Person>

		fun findAllByName(): Flow<Person>

		suspend fun findSuspendByName(): List<Person>
	}

	@Test // DATACASS-771
	internal fun `should consider methods returning Flow as collection queries`() {

		val method = PersonRepository::class.java.getMethod("findAllByName")
		val queryMethod = ReactiveCassandraQueryMethod(method, DefaultRepositoryMetadata(PersonRepository::class.java), projectionFactory, CassandraMappingContext())

		assertThat(queryMethod.isCollectionQuery).isTrue()
	}

	@Test // DATACASS-771
	internal fun `should consider suspended methods returning Flow as collection queries`() {

		val method = PersonRepository::class.java.getMethod("findSuspendAllByName", Continuation::class.java)
		val queryMethod = ReactiveCassandraQueryMethod(method, DefaultRepositoryMetadata(PersonRepository::class.java), projectionFactory, CassandraMappingContext())

		assertThat(queryMethod.isCollectionQuery).isTrue()
	}

	@Test // DATACASS-806
	internal fun `should consider suspended methods returning List as collection queries`() {

		val method = PersonRepository::class.java.getMethod("findSuspendByName", Continuation::class.java)
		val queryMethod = ReactiveCassandraQueryMethod(method, DefaultRepositoryMetadata(PersonRepository::class.java), projectionFactory, CassandraMappingContext())

		assertThat(queryMethod.isCollectionQuery).isTrue()
	}
}
