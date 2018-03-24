/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import java.util.Arrays;
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.CreateUserTypeCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Element;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.Tuple;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.convert.CustomConversions;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Integration tests for mapped tuple values through {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MappingCassandraConverterTupleIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	private static AtomicBoolean initialized = new AtomicBoolean();

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.NONE;
		}

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { AllPossibleTypes.class.getPackage().getName() };
		}

		@Override
		public CustomConversions customConversions() {
			return new CassandraCustomConversions(
					Arrays.asList(new StringToCurrencyConverter(), new CurrencyToStringConverter()));
		}
	}

	@Autowired MappingCassandraConverter converter;
	@Autowired Session session;

	@Before
	public void setUp() {

		if (initialized.compareAndSet(false, true)) {

			this.session.execute("DROP TYPE IF EXISTS address;");
			this.session.execute("DROP TABLE IF EXISTS person;");

			CassandraMappingContext mappingContext = converter.getMappingContext();

			CreateUserTypeSpecification createAddress = mappingContext
					.getCreateUserTypeSpecificationFor(mappingContext.getRequiredPersistentEntity(AddressUserType.class));

			this.session.execute(CreateUserTypeCqlGenerator.toCql(createAddress));

			CreateTableSpecification createPerson = mappingContext
					.getCreateTableSpecificationFor(mappingContext.getRequiredPersistentEntity(Person.class));

			this.session.execute(CreateTableCqlGenerator.toCql(createPerson));
		} else {
			this.session.execute("TRUNCATE person;");
		}
	}

	@Test // DATACASS-523
	public void shouldInsertRowWithComplexTuple() {

		Person person = new Person();

		person.setId("foo");

		AddressUserType userType = new AddressUserType();

		userType.setZip("myzip");

		MappedTuple tuple = new MappedTuple();

		tuple.setAddressUserType(userType);
		tuple.setCurrency(Arrays.asList(Currency.getInstance("EUR"), Currency.getInstance("USD")));
		tuple.setName("bar");

		person.setMappedTuple(tuple);
		person.setMappedTuples(Collections.singletonList(tuple));

		Insert insert = QueryBuilder.insertInto("person");

		this.converter.write(person, insert);
		this.session.execute(insert);
	}

	@Test // DATACASS-523
	public void shouldReadRowWithComplexTuple() {

		this.session.execute("INSERT INTO person (id,mappedtuple,mappedtuples) VALUES ("
				+ "'foo'," //
				+ "({zip:'myzip'},['EUR','USD'],'bar')," //
				+ "[({zip:'myzip'},['EUR','USD'],'bar')]);\n");

		ResultSet resultSet = this.session.execute("SELECT * FROM person;");

		Person person = this.converter.read(Person.class, resultSet.one());

		assertThat(person.getMappedTuples()).hasSize(1);
		assertThat(person.getMappedTuple()).isNotNull();

		MappedTuple mappedTuple = person.getMappedTuple();

		assertThat(mappedTuple.getAddressUserType()).isNotNull();
		assertThat(mappedTuple.getAddressUserType().getZip()).isEqualTo("myzip");
		assertThat(mappedTuple.getName()).isEqualTo("bar");
		assertThat(mappedTuple.getCurrency()).containsSequence(Currency.getInstance("EUR"), Currency.getInstance("USD"));
	}

	@Data
	@Table
	static class Person {

		@Id private String id;

		MappedTuple mappedTuple;
		List<MappedTuple> mappedTuples;
	}

	@Data
	@Tuple
	static class MappedTuple {

		@Element(0) AddressUserType addressUserType;
		@Element(1) List<Currency> currency;
		@Element(2) String name;

	}

	@UserDefinedType("address")
	@Data
	static class AddressUserType {
		String zip;
	}

	private static class StringToCurrencyConverter implements Converter<String, Currency> {

		@Override
		public Currency convert(String source) {
			return Currency.getInstance(source);
		}
	}

	private static class CurrencyToStringConverter implements Converter<Currency, String> {

		@Override
		public String convert(Currency source) {
			return source.getCurrencyCode();
		}
	}
}
