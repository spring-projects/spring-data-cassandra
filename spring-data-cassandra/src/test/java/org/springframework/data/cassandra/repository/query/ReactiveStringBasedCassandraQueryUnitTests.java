/*
 * Copyright 2016-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;

/**
 * Unit tests for {@link StringBasedCassandraQuery}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveStringBasedCassandraQueryUnitTests {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock private Cluster cluster;
	@Mock private Configuration configuration;
	@Mock private ReactiveCassandraOperations operations;
	@Mock private ReactiveCqlOperations cqlOperations;
	@Mock private ReactiveSession reactiveSession;

	private MappingCassandraConverter converter;
	private ProjectionFactory factory;
	private RepositoryMetadata metadata;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() {

		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.converter = new MappingCassandraConverter(new CassandraMappingContext());
		this.factory = new SpelAwareProxyProjectionFactory();

		this.converter.afterPropertiesSet();

		when(operations.getConverter()).thenReturn(converter);
	}

	@Test // DATACASS-335
	public void bindsSimplePropertyCorrectly() throws Exception {

		ReactiveStringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "White");

		SimpleStatement stringQuery = cassandraQuery.createQuery(accessor);

		assertThat(stringQuery.toString()).isEqualTo("SELECT * FROM person WHERE lastname=?;");
		assertThat(stringQuery.getObject(0)).isEqualTo("White");
	}

	@Test // DATACASS-146
	public void shouldApplyQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().fetchSize(777).build();

		ReactiveStringBasedCassandraQuery cassandraQuery =
				getQueryMethod("findByLastname", QueryOptions.class, String.class);

		CassandraParametersParameterAccessor parameterAccessor =
				new CassandraParametersParameterAccessor(cassandraQuery.getQueryMethod(), queryOptions, "White");

		SimpleStatement actual = cassandraQuery.createQuery(parameterAccessor);

		assertThat(actual.toString()).isEqualTo("SELECT * FROM person WHERE lastname=?;");
		assertThat(actual.getObject(0)).isEqualTo("White");
		assertThat(actual.getFetchSize()).isEqualTo(777);
	}

	@Test // DATACASS-146
	public void shouldApplyConsistencyLevel() {

		ReactiveStringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);

		CassandraParametersParameterAccessor parameterAccessor =
				new CassandraParametersParameterAccessor(cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(parameterAccessor);

		assertThat(actual.toString()).isEqualTo("SELECT * FROM person WHERE lastname=?;");
		assertThat(actual.getObject(0)).isEqualTo("Matthews");
		assertThat(actual.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
	}

	private ReactiveStringBasedCassandraQuery getQueryMethod(String name, Class<?>... args) {

		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);

		ReactiveCassandraQueryMethod queryMethod =
				new ReactiveCassandraQueryMethod(method, metadata, factory, converter.getMappingContext());

		return new ReactiveStringBasedCassandraQuery(queryMethod, operations, PARSER,
				ExtensionAwareQueryMethodEvaluationContextProvider.DEFAULT);
	}

	@SuppressWarnings("unused")
	private interface SampleRepository extends Repository<Person, String> {

		@Query("SELECT * FROM person WHERE lastname=?0;")
		@Consistency(ConsistencyLevel.LOCAL_ONE)
		Person findByLastname(String lastname);

		@Query("SELECT * FROM person WHERE lastname=?0;")
		Person findByLastname(QueryOptions queryOptions, String lastname);

	}
}
