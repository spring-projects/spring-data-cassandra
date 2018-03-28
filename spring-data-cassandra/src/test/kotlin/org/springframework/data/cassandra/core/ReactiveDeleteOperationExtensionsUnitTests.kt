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
 * Unit tests for [ReactiveDeleteOperationExtensions].
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner::class)
class ReactiveDeleteOperationExtensionsUnitTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operations: ReactiveFluentCassandraOperations

	@Test // DATACASS-484
	fun `delete(KClass) extension should call its Java counterpart`() {

		operations.delete(Person::class);
		verify(operations).delete(Person::class.java)
	}

	@Test // DATACASS-484
	fun `delete() with reified type parameter extension should call its Java counterpart`() {

		operations.delete<Person>()
		verify(operations).delete(Person::class.java)
	}
}
