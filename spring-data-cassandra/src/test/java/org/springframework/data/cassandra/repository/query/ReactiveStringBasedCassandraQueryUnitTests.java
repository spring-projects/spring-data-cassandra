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
package org.springframework.data.cassandra.repository.query;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cql.core.ReactiveCqlOperations;
import org.springframework.data.cql.core.session.ReactiveSession;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.query.ExtensionAwareEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.SimpleStatement;

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
	@SuppressWarnings("unchecked")
	public void setUp() {

		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.converter = new MappingCassandraConverter(new CassandraMappingContext());
		this.factory = new SpelAwareProxyProjectionFactory();

		this.converter.afterPropertiesSet();
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

	private ReactiveStringBasedCassandraQuery getQueryMethod(String name, Class<?>... args) {

		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);
		ReactiveCassandraQueryMethod queryMethod = new ReactiveCassandraQueryMethod(method, metadata, factory,
				converter.getMappingContext());

		return new ReactiveStringBasedCassandraQuery(queryMethod, operations, PARSER,
				new ExtensionAwareEvaluationContextProvider());
	}

	private interface SampleRepository extends Repository<Person, String> {

		@Query("SELECT * FROM person WHERE lastname=?0;")
		Person findByLastname(String lastname);
	}
}
