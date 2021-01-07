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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Single;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.MapIdCassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Unit tests for {@link ReactivePartTreeCassandraQuery}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class ReactivePartTreeCassandraQueryUnitTests {

	@Mock ReactiveCassandraOperations mockCassandraOperations;
	@Mock UserTypeResolver userTypeResolver;

	private CassandraMappingContext mappingContext;

	@BeforeEach
	void setUp() {

		mappingContext = new CassandraMappingContext();
		mappingContext.setUserTypeResolver(userTypeResolver);

		when(mockCassandraOperations.getConverter()).thenReturn(new MappingCassandraConverter(mappingContext));
	}

	@Test // DATACASS-335
	void shouldDeriveSimpleQuery() {

		String query = deriveQueryFromMethod("findByLastname", "foo");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE lastname='foo'");
	}

	@Test // DATACASS-335
	void shouldDeriveSimpleQueryWithoutNames() {

		String query = deriveQueryFromMethod("findPersonBy");

		assertThat(query).isEqualTo("SELECT * FROM person");
	}

	@Test // DATACASS-335
	void shouldDeriveAndQuery() {

		String query = deriveQueryFromMethod("findByFirstnameAndLastname", "foo", "bar");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='foo' AND lastname='bar'");
	}

	@Test // DATACASS-376
	void shouldAllowFiltering() {

		String query = deriveQueryFromMethod("findPersonByFirstname", "foo");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='foo' ALLOW FILTERING");
	}

	@Test // DATACASS-335, DATACASS-313
	void usesDynamicProjection() {

		String query = deriveQueryFromMethod("findDynamicallyProjectedBy", PersonProjection.class);

		assertThat(query).isEqualTo("SELECT lastname,firstname FROM person");
	}

	@Test // DATACASS-146
	void shouldApplyQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().pageSize(777).build();
		SimpleStatement statement = deriveQueryFromMethod(Repo.class, "findByFirstname",
				new Class[] { QueryOptions.class, String.class }, queryOptions, "Walter");

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person WHERE firstname=?");
		assertThat(statement.getPageSize()).isEqualTo(777);
	}

	@Test // DATACASS-146
	void shouldApplyConsistencyLevel() {

		SimpleStatement statement = deriveQueryFromMethod(Repo.class, "findPersonBy", new Class[0]);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person");
		assertThat(statement.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_ONE);
	}

	@Test // DATACASS-512
	void shouldCreateCountQuery() {

		SimpleStatement statement = deriveQueryFromMethod(PartTreeCassandraQueryUnitTests.Repo.class, "countBy",
				new Class[0]);

		assertThat(statement.getQuery()).isEqualTo("SELECT count(1) FROM person");
	}

	@Test // DATACASS-611
	void shouldCreateDeleteQuery() {

		SimpleStatement statement = deriveQueryFromMethod(PartTreeCassandraQueryUnitTests.Repo.class, "deleteAllByLastname",
				new Class[] { String.class }, "Walter");

		assertThat(statement.getQuery()).isEqualTo("DELETE FROM person WHERE lastname=?");
	}

	@Test // DATACASS-512
	void shouldCreateExistsQuery() {

		SimpleStatement statement = deriveQueryFromMethod(PartTreeCassandraQueryUnitTests.Repo.class, "existsBy",
				new Class[0]);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person LIMIT 1");
	}

	private String deriveQueryFromMethod(String method, Object... args) {

		Class<?>[] types = new Class<?>[args.length];

		for (int i = 0; i < args.length; i++) {
			types[i] = ClassUtils.getUserClass(args[i].getClass());
		}

		SimpleStatement statement = deriveQueryFromMethod(Repo.class, method, types, args);
		String query = statement.getQuery();

		List<Object> positionalValues = statement.getPositionalValues();

		for (Object positionalValue : positionalValues) {

			query = query.replaceFirst("\\?",
					positionalValue != null
							? CodecRegistry.DEFAULT.codecFor((Class) positionalValue.getClass()).format(positionalValue)
							: "NULL");
		}

		return query;
	}

	private SimpleStatement deriveQueryFromMethod(Class<?> repositoryInterface, String method, Class<?>[] types,
			Object... args) {

		ReactivePartTreeCassandraQuery partTreeQuery = createQueryForMethod(repositoryInterface, method, types);

		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(partTreeQuery.getQueryMethod(),
				args);

		return partTreeQuery.createQuery(new ConvertingParameterAccessor(mockCassandraOperations.getConverter(), accessor))
				.block();
	}

	private ReactivePartTreeCassandraQuery createQueryForMethod(Class<?> repositoryInterface, String methodName,
			Class<?>... paramTypes) {
		Class<?>[] userTypes = Arrays.stream(paramTypes)//
				.map(it -> it.getName().contains("Mockito") ? it.getSuperclass() : it)//
				.toArray(size -> new Class<?>[size]);
		try {
			Method method = repositoryInterface.getMethod(methodName, userTypes);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			ReactiveCassandraQueryMethod queryMethod = new ReactiveCassandraQueryMethod(method,
					new DefaultRepositoryMetadata(repositoryInterface), factory, mappingContext);

			return new ReactivePartTreeCassandraQuery(queryMethod, mockCassandraOperations);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unused")
	interface Repo extends MapIdCassandraRepository<Person> {

		@Query()
		Flux<Person> findByLastname(String lastname);

		Flux<Person> findByFirstnameAndLastname(String firstname, String lastname);

		Flux<Person> findPersonByFirstnameAndLastname(String firstname, String lastname);

		Flux<Person> findByFirstname(QueryOptions queryOptions, String firstname);

		Mono<Long> countBy();

		Mono<Boolean> deleteAllByLastname(String lastname);

		Mono<Boolean> existsBy();

		@Consistency(DefaultConsistencyLevel.LOCAL_ONE)
		Flux<Person> findPersonBy();

		@Query(allowFiltering = true)
		Flux<Person> findPersonByFirstname(String name);

		Mono<PersonProjection> findPersonProjectedBy();

		<T> Single<T> findDynamicallyProjectedBy(Class<T> type);
	}

	private interface PersonProjection {

		String getFirstname();

		String getLastname();
	}
}
