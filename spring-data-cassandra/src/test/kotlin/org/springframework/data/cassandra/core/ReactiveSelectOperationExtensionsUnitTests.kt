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
import org.springframework.data.cassandra.domain.User

/**
 * Unit tests for [ReactiveSelectOperationExtensions].
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner::class)
class ReactiveSelectOperationExtensionsUnitTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operations: ReactiveFluentCassandraOperations

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operationWithProjection: ReactiveSelectOperation.SelectWithProjection<User>

	@Test // DATACASS-484
	fun `query(KClass) extension should call its Java counterpart`() {

		operations.query(Person::class);
		verify(operations).query(Person::class.java)
	}

	@Test // DATACASS-484
	fun `query() with reified type parameter extension should call its Java counterpart`() {

		operations.query<Person>()
		verify(operations).query(Person::class.java)
	}

	@Test // DATACASS-484
	fun `asType(KClass) extension should call its Java counterpart`() {

		operationWithProjection.asType(User::class);
		verify(operationWithProjection).`as`(User::class.java)
	}

	@Test // DATACASS-484
	fun `asType() with reified type parameter extension should call its Java counterpart`() {

		operationWithProjection.asType<User>();
		verify(operationWithProjection).`as`(User::class.java)
	}
}
