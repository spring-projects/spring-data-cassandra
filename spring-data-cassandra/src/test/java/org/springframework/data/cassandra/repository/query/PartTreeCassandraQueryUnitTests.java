/*
 * Copyright 2016-2025 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;

/**
 * Unit tests for {@link PartTreeCassandraQuery}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PartTreeCassandraQueryUnitTests {

	@Mock CassandraOperations mockCassandraOperations;
	@Mock UserTypeResolver userTypeResolverMock;
	@Mock UserDefinedType userTypeMock;
	@Mock AttachmentPoint attachmentPoint;
	private DefaultUdtValue udtValue;

	private CassandraMappingContext mappingContext;
	private CassandraConverter converter;

	@BeforeEach
	void setUp() {

		this.mappingContext = new CassandraMappingContext();
		this.mappingContext.setUserTypeResolver(userTypeResolverMock);
		when(userTypeResolverMock.resolveType(any())).thenReturn(userTypeMock);

		this.converter = new MappingCassandraConverter(mappingContext);

		when(mockCassandraOperations.getConverter()).thenReturn(converter);
		when(attachmentPoint.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
		when(attachmentPoint.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);

		when(userTypeMock.copy(anyBoolean())).thenReturn(userTypeMock);
		when(userTypeMock.getAttachmentPoint()).thenReturn(attachmentPoint);
		when(userTypeMock.getFieldNames())
				.thenReturn(Arrays.asList("city", "country").stream().map(CqlIdentifier::fromCql).collect(Collectors.toList()));
		when(userTypeMock.allIndicesOf("city")).thenReturn(Collections.singletonList(0));
		when(userTypeMock.allIndicesOf("country")).thenReturn(Collections.singletonList(1));
		when(userTypeMock.getFieldTypes()).thenReturn(Arrays.asList(DataTypes.TEXT, DataTypes.TEXT));

		udtValue = new DefaultUdtValue(userTypeMock);
		when(userTypeMock.newValue()).thenReturn(udtValue);
	}

	@Test // DATACASS-7
	void shouldDeriveSimpleQuery() {

		String query = deriveQueryFromMethod("findByLastname", "foo");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE lastname='foo'");
	}

	@Test // DATACASS-511
	void shouldDeriveLimitingQuery() {

		String query = deriveQueryFromMethod("findTop3By");

		assertThat(query).isEqualTo("SELECT * FROM person LIMIT 3");
	}

	@Test // DATACASS-7
	void shouldDeriveSimpleQueryWithoutNames() {

		String query = deriveQueryFromMethod("findPersonBy");

		assertThat(query).isEqualTo("SELECT * FROM person");
	}

	@Test // DATACASS-7
	void shouldDeriveAndQuery() {

		String query = deriveQueryFromMethod("findByFirstnameAndLastname", "foo", "bar");

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname='foo' AND lastname='bar'");
	}

	@Test // DATACASS-7, DATACASS-313
	void usesDynamicProjection() {

		String query = deriveQueryFromMethod("findDynamicallyProjectedBy", PersonProjection.class);

		assertThat(query).isEqualTo("SELECT firstname,lastname FROM person");
	}

	@Test // DATACASS-479, DATACASS-313
	void usesProjectionQueryHiddenField() {

		String query = deriveQueryFromMethod("findPersonProjectedByNickname", "foo");

		assertThat(query).isEqualTo("SELECT firstname,lastname FROM person WHERE nickname='foo'");
	}

	@Test // DATACASS-357
	void shouldDeriveFieldInCollectionQuery() {

		String query = deriveQueryFromMethod(Repo.class, "findByFirstnameIn", new Class[] { Collection.class },
				Arrays.asList("Hank", "Walter")).getQuery();

		assertThat(query).isEqualTo("SELECT * FROM person WHERE firstname IN ?");
	}

	@Test // DATACASS-172
	void shouldDeriveSimpleQueryWithMappedUDT() {

		SimpleStatement query = deriveQueryFromMethod(Repo.class, "findByMainAddress", new Class[] { AddressType.class },
				new AddressType());

		assertThat(query.getQuery()).isEqualTo("SELECT * FROM person WHERE mainaddress=?");
	}

	@Test // DATACASS-172
	void shouldDeriveSimpleQueryWithUDTValue() {

		String query = deriveQueryFromMethod(Repo.class, "findByMainAddress", new Class[] { UdtValue.class }, udtValue)
				.getQuery();

		assertThat(query).isEqualTo("SELECT * FROM person WHERE mainaddress=?");
	}

	@Test // DATACASS-357
	void shouldDeriveUdtInCollectionQuery() {

		String query = deriveQueryFromMethod(Repo.class, "findByMainAddressIn", new Class[] { Collection.class },
				Collections.singleton(udtValue)).getQuery();

		assertThat(query).isEqualTo("SELECT * FROM person WHERE mainaddress IN ?");
	}

	@Test // DATACASS-343
	void shouldRenderMappedColumnNamesForCompositePrimaryKey() {

		SimpleStatement query = deriveQueryFromMethod(GroupRepository.class, "findByIdHashPrefix",
				new Class[] { String.class }, "foo");

		assertThat(query.getQuery()).isEqualTo("SELECT * FROM group WHERE hash_prefix=?");
	}

	@Test // DATACASS-376
	void shouldAllowFiltering() {

		SimpleStatement query = deriveQueryFromMethod(Repo.class, "findByFirstname", new Class[] { String.class }, "foo");

		assertThat(query.getQuery()).isEqualTo("SELECT * FROM person WHERE firstname=? ALLOW FILTERING");
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

		SimpleStatement statement = deriveQueryFromMethod(Repo.class, "countBy", new Class[0]);

		assertThat(statement.getQuery()).isEqualTo("SELECT count(1) FROM person");
	}

	@Test // DATACASS-611
	void shouldCreateDeleteQuery() {

		SimpleStatement statement = deriveQueryFromMethod(Repo.class, "deleteAllByLastname", new Class[] { String.class },
				"Walter");

		assertThat(statement.getQuery()).isEqualTo("DELETE FROM person WHERE lastname=?");
	}

	@Test // DATACASS-512, GH-1401
	void shouldCreateExistsQuery() {

		SimpleStatement statement = deriveQueryFromMethod(Repo.class, "existsBy", new Class[0]);

		assertThat(statement.getQuery()).isEqualTo("SELECT * FROM person LIMIT ?");
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
							: "null");
		}

		return query;
	}

	private SimpleStatement deriveQueryFromMethod(Class<?> repositoryInterface, String method, Class<?>[] types,
			Object... args) {

		PartTreeCassandraQuery partTreeQuery = createQueryForMethod(repositoryInterface, method, types);

		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(partTreeQuery.getQueryMethod(),
				args);

		return partTreeQuery.createQuery(
				new ConvertingParameterAccessor(mockCassandraOperations.getConverter(), accessor));
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

		@Consistency(DefaultConsistencyLevel.LOCAL_ONE)
		Person findPersonBy();

		Person findByMainAddress(AddressType address);

		Person findByMainAddress(UdtValue udtValue);

		Person findByMainAddressIn(Collection<AddressType> address);

		Person findByFirstnameIn(Collection<String> firstname);

		long countBy();

		boolean deleteAllByLastname(String lastname);

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
