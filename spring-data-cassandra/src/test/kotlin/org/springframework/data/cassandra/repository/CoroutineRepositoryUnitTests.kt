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
package org.springframework.data.cassandra.repository

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.annotation.Id
import org.springframework.data.cassandra.ReactiveResultSet
import org.springframework.data.cassandra.core.ReactiveCassandraOperations
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFactory
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import reactor.core.publisher.Mono

/**
 * Unit tests for Kotlin Coroutine repositories.
 *
 * @author Mark Paluch
 */
class CoroutineRepositoryUnitTests {

	val operations = mockk<ReactiveCassandraOperations>(relaxed = true)
	val cqlOperations = mockk<ReactiveCqlOperations>(relaxed = true)
	val resultSet = mockk<ReactiveResultSet>(relaxed = true)
	lateinit var repositoryFactory: ReactiveCassandraRepositoryFactory

	@BeforeEach
	fun before() {

		every { operations.getConverter() } returns MappingCassandraConverter(CassandraMappingContext())
		every { operations.reactiveCqlOperations } returns cqlOperations
		repositoryFactory = ReactiveCassandraRepositoryFactory(operations)
	}

	@Test // DATACASS-791
	fun `should discard result of suspended query method without result`() {

		every { resultSet.wasApplied() } returns true
		every { operations.execute(any()) } returns Mono.just(resultSet)

		val repository = repositoryFactory.getRepository(PersonRepository::class.java)

		runBlocking {
			repository.deleteAllByName("foo")
		}
	}

	interface PersonRepository : CoroutineCrudRepository<Person, Long> {

		suspend fun deleteAllByName(name: String)
	}

	data class Person(@Id var id: Long, var name: String)
}
