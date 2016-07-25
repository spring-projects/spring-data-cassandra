/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit tests for {@link CassandraQueryMethod}.
 *
 * @author Mark Paluch
 */
public class CassandraQueryMethodUnitTests {

	CassandraMappingContext context;

	@Before
	public void setUp() {
		context = new BasicCassandraMappingContext();
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void detectsCollectionFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		CassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");
		CassandraEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType(), is(typeCompatibleWith(Person.class)));
		assertThat(metadata.getTableName().toCql(), is("person"));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullMappingContext() throws Exception {

		Method method = SampleRepository.class.getMethod("method");

		new CassandraQueryMethod(method, new DefaultRepositoryMetadata(SampleRepository.class),
				new SpelAwareProxyProjectionFactory(), null);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void considersMethodAsCollectionQuery() throws Exception {

		CassandraQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");

		assertThat(queryMethod.isCollectionQuery(), is(true));
	}

	private CassandraQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters) throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new CassandraQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	@SuppressWarnings("unused")
	interface SampleRepository extends Repository<Person, Long> {

		List<Person> method();

	}
}
