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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.cassandra.domain.AddressType;
import org.springframework.data.cassandra.domain.Person;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.support.UserDefinedTypeBuilder;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Unit tests for {@link StringBasedCassandraQuery}.
 *
 * @author Matthew T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class StringBasedCassandraQueryUnitTests {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock private CassandraOperations operations;
	@Mock private UdtValue udtValue;
	@Mock private UserTypeResolver userTypeResolver;

	private RepositoryMetadata metadata;
	private MappingCassandraConverter converter;
	private ProjectionFactory factory;

	@BeforeEach
	void setUp() {

		CassandraMappingContext mappingContext = new CassandraMappingContext();

		mappingContext.setUserTypeResolver(userTypeResolver);

		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.converter = new MappingCassandraConverter(mappingContext);
		this.factory = new SpelAwareProxyProjectionFactory();

		this.converter.afterPropertiesSet();

		when(operations.getConverter()).thenReturn(converter);
	}

	@Test // DATACASS-117
	void bindsIndexParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
	}

	@Test // DATACASS-259
	void bindsIndexParameterForComposedQueryAnnotationCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByComposedQueryAnnotation", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
	}

	@Test // DATACASS-117
	void bindsAndEscapesIndexParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Mat\th'ew\"s");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Mat\th'ew\"s");
	}

	@Test // DATACASS-117, DATACASS-454
	void bindsAndEscapesBytesIndexParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }));

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.isIdempotent()).isTrue();
		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo(ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }));
	}

	@Test // DATACASS-454
	void shouldConsiderNonIdempotentOverride() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("nonIdempotentSelect", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.isIdempotent()).isFalse();
	}

	@Test // DATACASS-454
	void shouldNotApplyIdempotencyToNonSelectStatement() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("nonIdempotentDelete");
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod());

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.isIdempotent()).isNull();
	}

	@Test // DATACASS-117
	void bindsIndexParameterInListCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastNameIn", Collection.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), Arrays.asList("White", "Heisenberg"));

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname IN (?);");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo(Arrays.asList("White", "Heisenberg"));
	}

	@Test // DATACASS-117
	void bindsIndexParameterIsListCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastNamesAndAge", Collection.class, int.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), Arrays.asList("White", "Heisenberg"), 42);

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastnames = [?] AND age = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo(Arrays.asList("White", "Heisenberg"));
		assertThat(actual.getPositionalValues().get(1)).isEqualTo(42);
	}

	@Test // DATACASS-117
	void referencingUnknownIndexedParameterShouldFail() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByOutOfBoundsLastNameShouldFail", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Hello");

		assertThatExceptionOfType(QueryCreationException.class).isThrownBy(() -> cassandraQuery.createQuery(accessor));
	}

	@Test // DATACASS-117
	void referencingUnknownNamedParameterShouldFail() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByUnknownParameterLastNameShouldFail", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Hello");

		assertThatExceptionOfType(QueryCreationException.class).isThrownBy(() -> cassandraQuery.createQuery(accessor));
	}

	@Test // DATACASS-117
	void bindsIndexParameterInSetCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastNameIn", Collection.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), new HashSet<>(Arrays.asList("White", "Heisenberg")));

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname IN (?);");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo(new HashSet<>(Arrays.asList("White", "Heisenberg")));
	}

	@Test // DATACASS-117
	void bindsNamedParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByNamedParameter", String.class, String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Walter", "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
	}

	@Test // DATACASS-117
	void bindsIndexExpressionParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByIndexExpressionParameter", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
	}

	@Test // DATACASS-117
	void bindsExpressionParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByExpressionParameter", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
	}

	@Test // DATACASS-117
	void bindsConditionalExpressionParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByConditionalExpressionParameter", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Woohoo");

		accessor = new CassandraParametersParameterAccessor(cassandraQuery.getQueryMethod(), "Walter");

		actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Walter");
	}

	@Test // DATACASS-117
	void bindsReusedParametersCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastnameUsedTwice", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ? or firstname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
		assertThat(actual.getPositionalValues().get(1)).isEqualTo("Matthews");
	}

	@Test // DATACASS-117
	void bindsMultipleParametersCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastnameAndFirstname", String.class, String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews", "John");

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname=? AND firstname=?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
		assertThat(actual.getPositionalValues().get(1)).isEqualTo("John");
	}

	@Test // DATACASS-296
	void bindsConvertedParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByCreatedDate", LocalDate.class);
		CassandraParameterAccessor accessor = new ConvertingParameterAccessor(converter,
				new CassandraParametersParameterAccessor(cassandraQuery.getQueryMethod(), LocalDate.of(2010, 7, 4)));

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE createdDate=?;");
		assertThat(actual.getPositionalValues().get(0)).isInstanceOf(LocalDate.class);
		assertThat(actual.getPositionalValues().get(0).toString()).isEqualTo("2010-07-04");
	}

	@Test // DATACASS-172
	void bindsMappedUdtPropertyCorrectly() throws Exception {

		UserDefinedType addressType = UserDefinedTypeBuilder.forName("address").withField("city", DataTypes.TEXT)
				.withField("country", DataTypes.TEXT).build();

		when(userTypeResolver.resolveType(CqlIdentifier.fromCql("address"))).thenReturn(addressType);

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByMainAddress", AddressType.class);
		CassandraParameterAccessor accessor = new ConvertingParameterAccessor(converter,
				new CassandraParametersParameterAccessor(cassandraQuery.getQueryMethod(), new AddressType()));

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE address=?;");
		assertThat(actual.getPositionalValues().get(0)).isInstanceOf(UdtValue.class);
	}

	@Test // DATACASS-172
	void bindsUdtValuePropertyCorrectly() throws Exception {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByMainAddress", UdtValue.class);

		UserDefinedType addressType = UserDefinedTypeBuilder.forName("address").withField("city", DataTypes.TEXT)
				.withField("country", DataTypes.TEXT).build();
		when(udtValue.getType()).thenReturn(addressType);

		CassandraParameterAccessor accessor = new ConvertingParameterAccessor(converter,
				new CassandraParametersParameterAccessor(cassandraQuery.getQueryMethod(), udtValue));

		SimpleStatement actual = cassandraQuery.createQuery(accessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE address=?;");
		assertThat(actual.getPositionalValues().get(0).toString()).isEqualTo("udtValue");
	}

	@Test // DATACASS-146
	void shouldApplyQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().pageSize(777).build();

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", QueryOptions.class, String.class);

		CassandraParametersParameterAccessor parameterAccessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), queryOptions, "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(parameterAccessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
		assertThat(actual.getPageSize()).isEqualTo(777);
	}

	@Test // DATACASS-146
	void shouldApplyConsistencyLevel() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);

		CassandraParametersParameterAccessor parameterAccessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		SimpleStatement actual = cassandraQuery.createQuery(parameterAccessor);

		assertThat(actual.getQuery()).isEqualTo("SELECT * FROM person WHERE lastname = ?;");
		assertThat(actual.getPositionalValues().get(0)).isEqualTo("Matthews");
		assertThat(actual.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_ONE);
	}

	private StringBasedCassandraQuery getQueryMethod(String name, Class<?>... args) {

		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);

		CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, factory,
				converter.getMappingContext());

		return new StringBasedCassandraQuery(queryMethod, operations, PARSER,
				ExtensionAwareQueryMethodEvaluationContextProvider.DEFAULT);
	}

	@SuppressWarnings("unused")
	private interface SampleRepository extends Repository<Person, String> {

		@Query(value = "SELECT * FROM person WHERE lastname = ?0;")
		@Consistency(DefaultConsistencyLevel.LOCAL_ONE)
		Person findByLastname(String lastname);

		@Query(value = "SELECT * FROM person WHERE lastname = ?0;", idempotent = Query.Idempotency.NON_IDEMPOTENT)
		@Consistency(DefaultConsistencyLevel.LOCAL_ONE)
		Person nonIdempotentSelect(String lastname);

		@Query(value = "DELETE FROM person")
		@Consistency(DefaultConsistencyLevel.LOCAL_ONE)
		Person nonIdempotentDelete();

		@Query("SELECT * FROM person WHERE lastname = ?0;")
		Person findByLastname(QueryOptions queryOptions, String lastname);

		@Query("SELECT * FROM person WHERE lastname = ?0 or firstname = ?0;")
		Person findByLastnameUsedTwice(String lastname);

		@Query("SELECT * FROM person WHERE lastname = :lastname;")
		Person findByNamedParameter(@Param("another") String another, @Param("lastname") String lastname);

		@Query("SELECT * FROM person WHERE lastname = :#{[0]};")
		Person findByIndexExpressionParameter(String lastname);

		@Query("SELECT * FROM person WHERE lastnames = [?0] AND age = ?1;")
		Person findByLastNamesAndAge(Collection<String> lastname, int age);

		@Query("SELECT * FROM person WHERE lastname = ?0 AND age = ?2;")
		Person findByOutOfBoundsLastNameShouldFail(String lastname);

		@Query("SELECT * FROM person WHERE lastname = :unknown;")
		Person findByUnknownParameterLastNameShouldFail(String lastname);

		@Query("SELECT * FROM person WHERE lastname IN (?0);")
		Person findByLastNameIn(Collection<String> lastNames);

		@Query("SELECT * FROM person WHERE lastname = :#{#lastname};")
		Person findByExpressionParameter(@Param("lastname") String lastname);

		@Query("SELECT * FROM person WHERE lastname = :#{#lastname == 'Matthews' ? 'Woohoo' : #lastname};")
		Person findByConditionalExpressionParameter(@Param("lastname") String lastname);

		@Query("SELECT * FROM person WHERE lastname=?0 AND firstname=?1;")
		Person findByLastnameAndFirstname(String lastname, String firstname);

		@Query("SELECT * FROM person WHERE createdDate=?0;")
		Person findByCreatedDate(LocalDate createdDate);

		@Query("SELECT * FROM person WHERE address=?0;")
		Person findByMainAddress(AddressType address);

		@Query("SELECT * FROM person WHERE address=?0;")
		Person findByMainAddress(UdtValue UdtValue);

		@ComposedQueryAnnotation
		Person findByComposedQueryAnnotation(String lastname);

	}

	@Retention(RetentionPolicy.RUNTIME)
	@Query("SELECT * FROM person WHERE lastname = ?0;")
	private @interface ComposedQueryAnnotation {
	}

}
