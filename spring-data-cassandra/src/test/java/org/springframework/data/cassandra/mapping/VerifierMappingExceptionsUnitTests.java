/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mapping.model.MappingException;

/**
 * Unit tests for {@link VerifierMappingExceptions}.
 * 
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class VerifierMappingExceptionsUnitTests {

	@Mock CassandraPersistentEntity<?> entityMock;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {

		when(entityMock.getType()).thenReturn((Class) VerifierMappingExceptionsUnitTests.class);

	}

	@Test // DATACASS-258
	public void testDeprecatedMutability() {

		VerifierMappingExceptions exceptions = new VerifierMappingExceptions(entityMock, "err");
		exceptions.add(new MappingException("my error"));

		assertThat(exceptions.toString()).contains("my error");
	}
}
