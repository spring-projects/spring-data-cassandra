/*
 * Copyright 2016-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
import org.springframework.data.repository.query.ReactiveExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.data.spel.spi.ReactiveEvaluationContextExtension;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Unit tests for {@link StringBasedCassandraQuery}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class ReactiveStringBasedCassandraQueryUnitTests {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock private ReactiveCassandraOperations operations;
	@Mock private ReactiveCqlOperations cqlOperations;
	@Mock private ReactiveSession reactiveSession;

	private MappingCassandraConverter converter;
	private ProjectionFactory factory;
	private RepositoryMetadata metadata;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() {

		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.converter = new MappingCassandraConverter(new CassandraMappingContext());
		this.factory = new SpelAwareProxyProjectionFactory();

		this.converter.afterPropertiesSet();

		when(operations.getConverter()).thenReturn(converter);
	}

	@Test // DATACASS-335
	void bindsSimplePropertyCorrectly() {

		ReactiveStringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "White");

		SimpleStatement actual = cassandraQuery.createQuery(accessor).block();

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname=?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("White");
	}

	@Test // DATACASS-146
	void shouldApplyQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().pageSize(777).build();

		ReactiveStringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", QueryOptions.class,
				String.class);

		CassandraParametersParameterAccessor parameterAccessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), queryOptions, "White");

		SimpleStatement actual = cassandraQuery.createQuery(parameterAccessor).block();

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname=?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("White");
		assertThat(actual.getPageSize()).isEqualTo(777);
	}

	@Test // DATACASS-146
	void shouldApplyConsistencyLevel() {

		ReactiveStringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);

		CassandraParametersParameterAccessor parameterAccessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(parameterAccessor).block();

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname=?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
		assertThat(actual.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_ONE);
	}

	@Test // DATACASS-788
	void shouldUseSpelExtension() {

		ReactiveStringBasedCassandraQuery cassandraQuery = getQueryMethod("findBySpel");

		CassandraParametersParameterAccessor parameterAccessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod());

		SimpleStatement actual = cassandraQuery.createQuery(parameterAccessor).block();

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname=?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Walter");
	}

	private ReactiveStringBasedCassandraQuery getQueryMethod(String name, Class<?>... args) {

		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);

		ReactiveCassandraQueryMethod queryMethod = new ReactiveCassandraQueryMethod(method, metadata, factory,
				converter.getMappingContext());

		ReactiveExtensionAwareQueryMethodEvaluationContextProvider provider = new ReactiveExtensionAwareQueryMethodEvaluationContextProvider(
				Arrays.asList(MyReactiveExtension.INSTANCE, MyDefunctExtension.INSTANCE));

		return new ReactiveStringBasedCassandraQuery(queryMethod, operations, PARSER,
				provider);
	}

	@SuppressWarnings("unused")
	private interface SampleRepository extends Repository<Person, String> {

		@Query("SELECT * FROM person WHERE lastname=?0;")
		@Consistency(DefaultConsistencyLevel.LOCAL_ONE)
		Person findByLastname(String lastname);

		@Query("SELECT * FROM person WHERE lastname=?0;")
		Person findByLastname(QueryOptions queryOptions, String lastname);

		@Query("SELECT * FROM person WHERE lastname=:#{getName()};")
		Person findBySpel();

	}

	public static class MyReactiveExtensionObject implements EvaluationContextExtension {

		public String getName() {
			return "Walter";
		}

		@Override
		public String getExtensionId() {
			return "ext-1";
		}

		@Nullable
		@Override
		public MyReactiveExtensionObject getRootObject() {
			return this;
		}
	}

	public static class DefunctExtensionObject implements EvaluationContextExtension {

		public String getPrincipal() {
			throw new IllegalStateException();
		}

		@Override
		public String getExtensionId() {
			return "ext-1";
		}

		@Nullable
		@Override
		public DefunctExtensionObject getRootObject() {
			throw new IllegalStateException();
		}
	}

	enum MyReactiveExtension implements ReactiveEvaluationContextExtension {

		INSTANCE;

		@Override
		public Mono<MyReactiveExtensionObject> getExtension() {
			return Mono.just(new MyReactiveExtensionObject());
		}

		@Override
		public String getExtensionId() {
			return "ext-1";
		}
	}

	enum MyDefunctExtension implements ReactiveEvaluationContextExtension {

		INSTANCE;

		@Override
		public Mono<DefunctExtensionObject> getExtension() {
			return Mono.error(new IllegalStateException());
		}

		@Override
		public String getExtensionId() {
			return "ext-2";
		}
	}
}
