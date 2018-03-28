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
package org.springframework.data.cassandra.core

import com.nhaarman.mockito_kotlin.verify
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Answers
import org.mockito.Mock
import org.mockito.junit.MockitoJUnitRunner
import org.springframework.data.cassandra.domain.Person

/**
 * Unit tests for [ExecutableInsertOperationExtensions].
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner::class)
class ExecutableInsertOperationExtensionsUnitTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operations: FluentCassandraOperations

	@Test // DATACASS-484
	fun `insert(KClass) extension should call its Java counterpart`() {

		operations.insert(Person::class);
		verify(operations).insert(Person::class.java)
	}

	@Test // DATACASS-484
	fun `insert() with reified type parameter extension should call its Java counterpart`() {

		operations.insert<Person>()
		verify(operations).insert(Person::class.java)
	}
}
