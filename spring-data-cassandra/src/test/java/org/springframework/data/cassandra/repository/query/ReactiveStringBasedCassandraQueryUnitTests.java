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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.ReactiveCqlOperations;
import org.springframework.cassandra.core.ReactiveSession;
import org.springframework.cassandra.core.ReactiveSessionCallback;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.Person;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.query.ExtensionAwareEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Unit tests for {@link StringBasedCassandraQuery}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveStringBasedCassandraQueryUnitTests {

	SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock ReactiveCassandraOperations operations;
	@Mock ReactiveCqlOperations cqlOperations;
	@Mock ReactiveSession reactiveSession;
	@Mock Cluster cluster;
	@Mock Configuration configuration;

	RepositoryMetadata metadata;
	MappingCassandraConverter converter;
	ProjectionFactory factory;

	@Before
	public void setUp() {

		when(operations.getConverter()).thenReturn(converter);
		when(operations.getReactiveCqlOperations()).thenReturn(cqlOperations);
		when(cqlOperations.execute(any(ReactiveSessionCallback.class))).thenAnswer(
				invocation -> ((ReactiveSessionCallback) invocation.getArguments()[0]).doInSession(reactiveSession));
		when(reactiveSession.getCluster()).thenReturn(cluster);
		when(cluster.getConfiguration()).thenReturn(configuration);
		when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);

		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.converter = new MappingCassandraConverter(new BasicCassandraMappingContext());
		this.factory = new SpelAwareProxyProjectionFactory();

		this.converter.afterPropertiesSet();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void bindsSimplePropertyCorrectly() throws Exception {

		ReactiveStringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "White");

		String stringQuery = cassandraQuery.createQuery(accessor);
		SimpleStatement actual = new SimpleStatement(stringQuery);

		String table = Person.class.getSimpleName().toLowerCase();
		Select expected = QueryBuilder.select().all().from(table);
		expected.setForceNoValues(true);
		expected.where(QueryBuilder.eq("lastname", "White"));

		assertThat(actual.getQueryString()).isEqualTo(expected.getQueryString());
	}

	private ReactiveStringBasedCassandraQuery getQueryMethod(String name, Class<?>... args) {

		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);
		CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, factory,
				converter.getMappingContext());
		return new ReactiveStringBasedCassandraQuery(queryMethod, operations, PARSER,
				new ExtensionAwareEvaluationContextProvider());
	}

	private interface SampleRepository extends Repository<Person, String> {

		@Query("SELECT * FROM person WHERE lastname=?0;")
		Person findByLastname(String lastname);
	}
}
