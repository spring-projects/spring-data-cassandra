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
import java.util.Arrays;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;

import rx.Single;

/**
 * Unit tests for {@link ReactivePartTreeCassandraQuery}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactivePartTreeCassandraQueryUnitTests {

	@Rule public ExpectedException exception = ExpectedException.none();

	@Mock ReactiveCassandraOperations mockCassandraOperations;
	@Mock UserTypeResolver userTypeResolver;

	private CassandraMappingContext mappingContext;

	@Before
	public void setUp() {

		mappingContext = new CassandraMappingContext();
		mappingContext.setUserTypeResolver(userTypeResolver);

		when(mockCassandraOperations.getConverter()).thenReturn(new MappingCassandraConverter(mappingContext));
	}

	@Test // DATACASS-335
	public void shouldDeriveSimpleQuery() {

		String query = deriveQueryFromMethod("findByLastname", "foo");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE lastname='foo';");
	}

	@Test // DATACASS-335
	public void shouldDeriveSimpleQueryWithoutNames() {

		String query = deriveQueryFromMethod("findPersonBy");

		assertThat(query).isEqualTo("SELECT * FROM person;");
	}

	@Test // DATACASS-335
	public void shouldDeriveAndQuery() {

		String query = deriveQueryFromMethod("findByFirstnameAndLastname", "foo", "bar");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='foo' AND lastname='bar';");
	}

	@Test // DATACASS-376
	public void shouldAllowFiltering() {

		String query = deriveQueryFromMethod("findPersonByFirstname", "foo");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='foo' ALLOW FILTERING;");
	}

	@Test // DATACASS-335
	public void usesDynamicProjection() {

		String query = deriveQueryFromMethod("findDynamicallyProjectedBy", PersonProjection.class);

		assertThat(query).isEqualTo("SELECT * FROM person;");
	}

	@Test // DATACASS-146
	public void shouldApplyQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().fetchSize(777).build();
		Statement statement = deriveQueryFromMethod(Repo.class, "findByFirstname",
				new Class[] { QueryOptions.class, String.class }, queryOptions, "Walter");

		assertThat(statement.toString()).isEqualTo("SELECT * FROM person WHERE firstname='Walter';");
		assertThat(statement.getFetchSize()).isEqualTo(777);
	}

	@Test // DATACASS-146
	public void shouldApplyConsistencyLevel() {

		Statement statement = deriveQueryFromMethod(Repo.class, "findPersonBy", new Class[0]);

		assertThat(statement.toString()).isEqualTo("SELECT * FROM person;");
		assertThat(statement.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
	}

	@Test // DATACASS-512
	public void shouldCreateCountQuery() {

		Statement statement = deriveQueryFromMethod(PartTreeCassandraQueryUnitTests.Repo.class, "countBy", new Class[0]);

		assertThat(statement.toString()).isEqualTo("SELECT COUNT(1) FROM person;");
	}

	@Test // DATACASS-512
	public void shouldCreateExistsQuery() {

		Statement statement = deriveQueryFromMethod(PartTreeCassandraQueryUnitTests.Repo.class, "existsBy", new Class[0]);

		assertThat(statement.toString()).isEqualTo("SELECT * FROM person LIMIT 1;");
	}

	private String deriveQueryFromMethod(String method, Object... args) {

		Class<?>[] types = new Class<?>[args.length];

		for (int i = 0; i < args.length; i++) {
			types[i] = ClassUtils.getUserClass(args[i].getClass());
		}

		return deriveQueryFromMethod(Repo.class, method, types, args).toString();
	}

	private Statement deriveQueryFromMethod(Class<?> repositoryInterface, String method, Class<?>[] types,
			Object... args) {

		ReactivePartTreeCassandraQuery partTreeQuery = createQueryForMethod(repositoryInterface, method, types);

		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(partTreeQuery.getQueryMethod(),
				args);

		return partTreeQuery.createQuery(new ConvertingParameterAccessor(mockCassandraOperations.getConverter(), accessor));
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

		Mono<Boolean> existsBy();

		@Consistency(ConsistencyLevel.LOCAL_ONE)
		Flux<Person> findPersonBy();

		@Query(allowFiltering = true)
		Flux<Person> findPersonByFirstname(String name);

		Mono<PersonProjection> findPersonProjectedBy();

		<T> Single<T> findDynamicallyProjectedBy(Class<T> type);
	}

	interface PersonProjection {

		String getFirstname();

		String getLastname();
	}
}
