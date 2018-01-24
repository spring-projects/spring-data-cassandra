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
import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.domain.AddressType;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.AllowFiltering;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.MapIdCassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

/**
 * Unit tests for {@link PartTreeCassandraQuery}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class PartTreeCassandraQueryUnitTests {

	@Rule public ExpectedException exception = ExpectedException.none();

	@Mock CassandraOperations mockCassandraOperations;
	@Mock UserTypeResolver userTypeResolverMock;
	@Mock UserType userTypeMock;
	@Mock UDTValue udtValueMock;

	CassandraMappingContext mappingContext;
	CassandraConverter converter;

	@Before
	public void setUp() {

		this.mappingContext = new CassandraMappingContext();
		this.mappingContext.setUserTypeResolver(userTypeResolverMock);

		this.converter = new MappingCassandraConverter(mappingContext);

		when(mockCassandraOperations.getConverter()).thenReturn(converter);
		when(udtValueMock.getType()).thenReturn(userTypeMock);
		when(userTypeMock.iterator()).thenReturn(Collections.emptyIterator());
	}

	@Test // DATACASS-7
	public void shouldDeriveSimpleQuery() {

		String query = deriveQueryFromMethod("findByLastname", "foo");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE lastname='foo';");
	}

	@Test // DATACASS-511
	public void shouldDeriveLimitingQuery() {

		String query = deriveQueryFromMethod("findTop3By");

		assertThat(query).isEqualTo("SELECT * FROM person LIMIT 3;");
	}

	@Test // DATACASS-7
	public void shouldDeriveSimpleQueryWithoutNames() {

		String query = deriveQueryFromMethod("findPersonBy");

		assertThat(query).isEqualTo("SELECT * FROM person;");
	}

	@Test // DATACASS-7
	public void shouldDeriveAndQuery() {

		String query = deriveQueryFromMethod("findByFirstnameAndLastname", "foo", "bar");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='foo' AND lastname='bar';");
	}

	@Test // DATACASS-7
	public void usesDynamicProjection() {

		String query = deriveQueryFromMethod("findDynamicallyProjectedBy", PersonProjection.class);

		assertThat(query).isEqualTo("SELECT * FROM person;");
	}

	@Test // DATACASS-479
	public void usesProjectionQueryHiddenField() {

		String query = deriveQueryFromMethod("findPersonProjectedByNickname", "foo");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE nickname='foo';");
	}

	@Test // DATACASS-357
	public void shouldDeriveFieldInCollectionQuery() {

		String query = deriveQueryFromMethod(Repo.class, "findByFirstnameIn", new Class[] { Collection.class },
				Arrays.asList("Hank", "Walter")).toString();

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname IN ('Hank','Walter');");
	}

	@Test // DATACASS-172
	public void shouldDeriveSimpleQueryWithMappedUDT() {

		when(userTypeResolverMock.resolveType(CqlIdentifier.of("address"))).thenReturn(userTypeMock);
		when(userTypeMock.newValue()).thenReturn(udtValueMock);

		String query = deriveQueryFromMethod("findByMainAddress", new AddressType());

		assertThat(query).isEqualTo("SELECT * FROM person WHERE mainaddress={};");
	}

	@Test // DATACASS-172
	public void shouldDeriveSimpleQueryWithUDTValue() {

		String query = deriveQueryFromMethod("findByMainAddress", udtValueMock);

		assertThat(query).isEqualTo("SELECT * FROM person WHERE mainaddress={};");
	}

	@Test // DATACASS-357
	public void shouldDeriveUdtInCollectionQuery() {

		String query = deriveQueryFromMethod(Repo.class, "findByMainAddressIn", new Class[] { Collection.class },
				Collections.singleton(udtValueMock)).toString();

		assertThat(query).isEqualTo("SELECT * FROM person WHERE mainaddress IN ({});");
	}

	@Test // DATACASS-343
	public void shouldRenderMappedColumnNamesForCompositePrimaryKey() {

		Statement query = deriveQueryFromMethod(GroupRepository.class, "findByIdHashPrefix", new Class[] { String.class },
				"foo");

		assertThat(query.toString()).isEqualTo("SELECT * FROM group WHERE hash_prefix='foo';");
	}

	@Test // DATACASS-376
	public void shouldAllowFiltering() {

		Statement query = deriveQueryFromMethod(Repo.class, "findByFirstname", new Class[] { String.class }, "foo");

		assertThat(query.toString()).isEqualTo("SELECT * FROM person WHERE firstname='foo' ALLOW FILTERING;");
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

		Statement statement = deriveQueryFromMethod(Repo.class, "countBy", new Class[0]);

		assertThat(statement.toString()).isEqualTo("SELECT COUNT(1) FROM person;");
	}

	@Test // DATACASS-512
	public void shouldCreateExistsQuery() {

		Statement statement = deriveQueryFromMethod(Repo.class, "existsBy", new Class[0]);

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

		PartTreeCassandraQuery partTreeQuery = createQueryForMethod(repositoryInterface, method, types);

		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(partTreeQuery.getQueryMethod(),
				args);

		return partTreeQuery.createQuery(new ConvertingParameterAccessor(mockCassandraOperations.getConverter(), accessor));
	}

	private PartTreeCassandraQuery createQueryForMethod(Class<?> repositoryInterface, String methodName,
			Class<?>... paramTypes) {
		Class<?>[] userTypes = Arrays.stream(paramTypes)//
				.map(it -> it.getName().contains("Mockito") ? it.getSuperclass() : it)//
				.toArray(size -> new Class<?>[size]);
		try {
			Method method = repositoryInterface.getMethod(methodName, userTypes);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			CassandraQueryMethod queryMethod = new CassandraQueryMethod(method,
					new DefaultRepositoryMetadata(repositoryInterface), factory, mappingContext);

			return new PartTreeCassandraQuery(queryMethod, mockCassandraOperations);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unused")
	interface GroupRepository extends MapIdCassandraRepository<Group> {

		Group findByIdHashPrefix(String hashPrefix);
	}

	@SuppressWarnings("unused")
	interface Repo extends MapIdCassandraRepository<Person> {

		@Query()
		Person findByLastname(String lastname);

		Person findTop3By();

		Person findByFirstnameAndLastname(String firstname, String lastname);

		Person findPersonByFirstnameAndLastname(String firstname, String lastname);

		Person findByFirstname(QueryOptions queryOptions, String firstname);

		@Consistency(ConsistencyLevel.LOCAL_ONE)
		Person findPersonBy();

		Person findByMainAddress(AddressType address);

		Person findByMainAddress(UDTValue udtValue);

		Person findByMainAddressIn(Collection<AddressType> address);

		Person findByFirstnameIn(Collection<String> firstname);

		long countBy();

		boolean existsBy();

		@AllowFiltering
		Person findByFirstname(String firstname);

		PersonProjection findPersonProjectedByNickname(String nickname);

		<T> T findDynamicallyProjectedBy(Class<T> type);
	}

	interface PersonProjection {

		String getFirstname();

		String getLastname();
	}
}
