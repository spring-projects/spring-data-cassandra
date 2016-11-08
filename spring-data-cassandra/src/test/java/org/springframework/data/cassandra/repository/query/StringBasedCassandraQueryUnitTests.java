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
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.Person;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.query.ExtensionAwareEvaluationContextProvider;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Unit tests for {@link StringBasedCassandraQuery}.
 *
 * @author Matthew T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class StringBasedCassandraQueryUnitTests {

	SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock CassandraOperations operations;
	@Mock Session session;
	@Mock Cluster cluster;
	@Mock Configuration configuration;

	RepositoryMetadata metadata;
	MappingCassandraConverter converter;
	ProjectionFactory factory;

	@Before
	public void setUp() {

		when(operations.getConverter()).thenReturn(converter);
		when(operations.getSession()).thenReturn(session);
		when(session.getCluster()).thenReturn(cluster);
		when(cluster.getConfiguration()).thenReturn(configuration);
		when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);

		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.converter = new MappingCassandraConverter(new BasicCassandraMappingContext());
		this.factory = new SpelAwareProxyProjectionFactory();

		this.converter.afterPropertiesSet();
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsIndexParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname = 'Matthews';");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsAndEscapesIndexParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Mat\th'ew\"s");

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname = 'Mat\th''ew\"s';");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsAndEscapesBytesIndexParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastname", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }));

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname = 0x01020304;");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsIndexParameterInListCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastNameIn", Collection.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), Arrays.asList("White", "Heisenberg"));

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname IN ('White','Heisenberg');");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsIndexParameterIsListCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastNamesAndAge", Collection.class, int.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), Arrays.asList("White", "Heisenberg"), 42);

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastnames = ['White','Heisenberg'] AND age = 42;");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test(expected = QueryCreationException.class)
	public void referencingUnknownIndexedParameterShouldFail() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByOutOfBoundsLastNameShouldFail", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Hello");

		cassandraQuery.createQuery(accessor);
	}

	/**
	 * @see DATACASS-117
	 */
	@Test(expected = QueryCreationException.class)
	public void referencingUnknownNamedParameterShouldFail() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByUnknownParameterLastNameShouldFail", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Hello");

		cassandraQuery.createQuery(accessor);
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsIndexParameterInSetCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastNameIn", Collection.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), new HashSet<String>(Arrays.asList("White", "Heisenberg")));

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname IN ('White','Heisenberg');");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsNamedParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByNamedParameter", String.class, String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Walter", "Matthews");

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname = 'Matthews';");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsIndexExpressionParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByIndexExpressionParameter", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname = 'Matthews';");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsExpressionParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByExpressionParameter", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname = 'Matthews';");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsConditionalExpressionParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByConditionalExpressionParameter", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname = 'Woohoo';");

		accessor = new CassandraParametersParameterAccessor(cassandraQuery.getQueryMethod(), "Walter");

		actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname = 'Walter';");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsReusedParametersCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastnameUsedTwice", String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews");

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE lastname='Matthews' or firstname = 'Matthews';");
	}

	/**
	 * @see DATACASS-117
	 */
	@Test
	public void bindsMultipleParametersCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByLastnameAndFirstname", String.class, String.class);
		CassandraParametersParameterAccessor accessor = new CassandraParametersParameterAccessor(
				cassandraQuery.getQueryMethod(), "Matthews", "John");

		String actual = cassandraQuery.createQuery(accessor);

		String table = Person.class.getSimpleName().toLowerCase();
		Select expected = QueryBuilder.select().all().from(table);
		expected.setForceNoValues(true);
		expected.where(QueryBuilder.eq("lastname", "Matthews")).and(QueryBuilder.eq("firstname", "John"));

		assertThat(actual).isEqualTo(expected.getQueryString());
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void bindsConvertedParameterCorrectly() {

		StringBasedCassandraQuery cassandraQuery = getQueryMethod("findByCreatedDate", LocalDate.class);
		CassandraParameterAccessor accessor = new ConvertingParameterAccessor(converter,
				new CassandraParametersParameterAccessor(cassandraQuery.getQueryMethod(), LocalDate.of(2010, 7, 4)));

		String actual = cassandraQuery.createQuery(accessor);

		assertThat(actual).isEqualTo("SELECT * FROM person WHERE createdDate='2010-07-04';");
	}

	private StringBasedCassandraQuery getQueryMethod(String name, Class<?>... args) {
		
		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);
		CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, factory,
				converter.getMappingContext());
		return new StringBasedCassandraQuery(queryMethod, operations, PARSER,
				new ExtensionAwareEvaluationContextProvider());
	}

	private interface SampleRepository extends Repository<Person, String> {

		@Query("SELECT * FROM person WHERE lastname = ?0;")
		Person findByLastname(String lastname);

		@Query("SELECT * FROM person WHERE lastname=?0 or firstname = ?0;")
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
	}
}
