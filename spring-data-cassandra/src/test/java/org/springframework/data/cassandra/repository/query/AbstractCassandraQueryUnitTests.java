/*
 * Copyright 2023-present the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import static org.mockito.Mockito.*;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit tests for {@link AbstractCassandraQuery}.
 *
 * @author Mark Paluch
 */
public class AbstractCassandraQueryUnitTests {

	CassandraMappingContext context = new CassandraMappingContext();
	CassandraOperations operations = mock(CassandraOperations.class);

	@BeforeEach
	void setUp() {
		when(operations.getConverter()).thenReturn(new MappingCassandraConverter(context));
	}

	@Test
	void shouldResolveDomainTypeForReturnedInterfaceInHierarchy() throws Exception {

		DefaultRepositoryMetadata metadata = new DefaultRepositoryMetadata(MyRepository.class);

		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		CassandraQueryMethod method = new CassandraQueryMethod(MyRepository.class.getMethod("findBy"), metadata, factory,
				context);

		PartTreeCassandraQuery cq = new PartTreeCassandraQuery(method, operations);
		cq.execute(new Object[0]);

		verify(operations).select(any(com.datastax.oss.driver.api.core.cql.Statement.class), eq(MyClass.class));
	}

	interface MyInterface {

	}

	static class MyClass implements MyInterface {

	}

	interface MyRepository extends CassandraRepository<MyClass, String> {

		Optional<MyInterface> findBy();
	}
}
