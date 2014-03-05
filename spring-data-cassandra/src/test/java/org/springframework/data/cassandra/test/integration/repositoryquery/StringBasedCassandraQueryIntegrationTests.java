/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repositoryquery;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.query.CassandraParametersParameterAccessor;
import org.springframework.data.cassandra.repository.query.CassandraQueryMethod;
import org.springframework.data.cassandra.repository.query.StringBasedCassandraQuery;
import org.springframework.data.repository.core.RepositoryMetadata;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Unit tests for {@link StringBasedCassandraQuery}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class StringBasedCassandraQueryIntegrationTests {

	@Mock
	CassandraOperations operations;
	@Mock
	RepositoryMetadata metadata;

	CassandraConverter converter;

	@Before
	public void setUp() {

		when(operations.getConverter()).thenReturn(converter);

		this.converter = new MappingCassandraConverter(new BasicCassandraMappingContext());
	}

	@Test
	public void bindsSimplePropertyCorrectly() throws Exception {

		Method method = SampleRepository.class.getMethod("findByLastname", String.class);
		CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, converter.getMappingContext());
		StringBasedCassandraQuery cassandraQuery = new StringBasedCassandraQuery(queryMethod, operations);
		CassandraParametersParameterAccessor accesor = new CassandraParametersParameterAccessor(queryMethod, "Matthews");

		String stringQuery = cassandraQuery.createQuery(accesor);
		SimpleStatement actual = new SimpleStatement(stringQuery);

		String table = Person.class.getSimpleName().toLowerCase();
		Select expected = QueryBuilder.select().all().from(table);
		expected.where(QueryBuilder.eq("lastname", "Matthews"));

		assertThat(actual.getQueryString(), is(expected.getQueryString()));
	}

	@Test
	public void bindsMultipleParametersCorrectly() throws Exception {

		Method method = SampleRepository.class.getMethod("findByLastnameAndFirstname", String.class, String.class);
		CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, converter.getMappingContext());
		StringBasedCassandraQuery cassandraQuery = new StringBasedCassandraQuery(queryMethod, operations);
		CassandraParametersParameterAccessor accesor = new CassandraParametersParameterAccessor(queryMethod, "Matthews",
				"John");

		String stringQuery = cassandraQuery.createQuery(accesor);
		SimpleStatement actual = new SimpleStatement(stringQuery);

		String table = Person.class.getSimpleName().toLowerCase();
		Select expected = QueryBuilder.select().all().from(table);
		expected.where(QueryBuilder.eq("lastname", "Matthews")).and(QueryBuilder.eq("firstname", "John"));

		assertThat(actual.getQueryString(), is(expected.getQueryString()));
	}

	private interface SampleRepository {

		@Query("SELECT * FROM person WHERE lastname='?0';")
		Person findByLastname(String lastname);

		@Query("SELECT * FROM person WHERE lastname='?0' AND firstname='?1';")
		Person findByLastnameAndFirstname(String lastname, String firstname);
	}
}
