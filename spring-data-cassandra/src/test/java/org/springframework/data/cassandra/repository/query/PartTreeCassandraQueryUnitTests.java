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
import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.UserTypeResolver;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.Address;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.Person;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.util.ClassUtils;

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

	BasicCassandraMappingContext mappingContext;
	CassandraConverter converter;

	@Before
	public void setUp() {

		this.mappingContext = new BasicCassandraMappingContext();
		this.mappingContext.setUserTypeResolver(userTypeResolverMock);

		this.converter = new MappingCassandraConverter(mappingContext);

		when(mockCassandraOperations.getConverter()).thenReturn(converter);
		when(udtValueMock.getType()).thenReturn(userTypeMock);
		when(userTypeMock.iterator()).thenReturn(Collections.<UserType.Field> emptyIterator());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void shouldDeriveSimpleQuery() {
		String query = deriveQueryFromMethod("findByLastname", "foo");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE lastname='foo';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void shouldDeriveSimpleQueryWithoutNames() {
		String query = deriveQueryFromMethod("findPersonBy");

		assertThat(query).isEqualTo("SELECT * FROM person;");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void shouldDeriveAndQuery() {
		String query = deriveQueryFromMethod("findByFirstnameAndLastname", "foo", "bar");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='foo' AND lastname='bar';");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-7">DATACASS-7</a>
	 */
	@Test
	public void usesDynamicProjection() {
		String query = deriveQueryFromMethod("findDynamicallyProjectedBy", PersonProjection.class);

		assertThat(query).isEqualTo("SELECT * FROM person;");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-172">DATACASS-172</a>
	 */
	@Test
	public void shouldDeriveSimpleQueryWithMappedUDT() {

		when(userTypeResolverMock.resolveType(CqlIdentifier.cqlId("address"))).thenReturn(userTypeMock);
		when(userTypeMock.newValue()).thenReturn(udtValueMock);

		String query = deriveQueryFromMethod("findByMainAddress", new Address());

		assertThat(query).isEqualTo("SELECT * FROM person WHERE mainaddress={};");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-172">DATACASS-172</a>
	 */
	@Test
	public void shouldDeriveSimpleQueryWithUDTValue() {

		when(userTypeResolverMock.resolveType(CqlIdentifier.cqlId("address"))).thenReturn(userTypeMock);
		when(userTypeMock.newValue()).thenReturn(udtValueMock);

		String query = deriveQueryFromMethod("findByMainAddress", udtValueMock);

		assertThat(query).isEqualTo("SELECT * FROM person WHERE mainaddress={};");
	}

	private String deriveQueryFromMethod(String method, Object... args) {
		Class<?>[] types = new Class<?>[args.length];

		for (int i = 0; i < args.length; i++) {
			types[i] = ClassUtils.getUserClass(args[i].getClass());
		}

		PartTreeCassandraQuery partTreeQuery = createQueryForMethod(method, types);

		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(partTreeQuery.getQueryMethod(),
				args);

		return partTreeQuery.createQuery(new ConvertingParameterAccessor(mockCassandraOperations.getConverter(), accessor));
	}

	private PartTreeCassandraQuery createQueryForMethod(String methodName, Class<?>... paramTypes) {
		try {
			Method method = Repo.class.getMethod(methodName, paramTypes);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, new DefaultRepositoryMetadata(Repo.class),
					factory, mappingContext);

			return new PartTreeCassandraQuery(queryMethod, mockCassandraOperations);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unused")
	interface Repo extends CassandraRepository<Person> {

		@Query()
		Person findByLastname(String lastname);

		Person findByFirstnameAndLastname(String firstname, String lastname);

		Person findPersonByFirstnameAndLastname(String firstname, String lastname);

		Person findByAge(Integer age);

		Person findPersonBy();

		Person findByMainAddress(Address address);

		Person findByMainAddress(UDTValue udtValue);

		PersonProjection findPersonProjectedBy();

		<T> T findDynamicallyProjectedBy(Class<T> type);

	}

	interface PersonProjection {

		String getFirstname();

		String getLastname();
	}
}
