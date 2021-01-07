/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import java.util.Arrays;
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.generator.CreateUserTypeCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Element;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.Tuple;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;
import org.springframework.data.cassandra.domain.AllPossibleTypes;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;

/**
 * Integration tests for mapped tuple values through {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig
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
		public CassandraCustomConversions customConversions() {
			return new CassandraCustomConversions(
					Arrays.asList(new StringToCurrencyConverter(), new CurrencyToStringConverter()));
		}
	}

	@Autowired MappingCassandraConverter converter;
	@Autowired CqlSession session;

	@BeforeEach
	void setUp() {

		if (initialized.compareAndSet(false, true)) {

			this.session.execute("DROP TYPE IF EXISTS address;");
			this.session.execute("DROP TABLE IF EXISTS person;");

			CassandraMappingContext mappingContext = converter.getMappingContext();
			SchemaFactory schemaFactory = new SchemaFactory(converter);

			CreateUserTypeSpecification createAddress = schemaFactory
					.getCreateUserTypeSpecificationFor(mappingContext.getRequiredPersistentEntity(AddressUserType.class));

			this.session.execute(CreateUserTypeCqlGenerator.toCql(createAddress));

			String ddl = "CREATE TABLE person (id text, " + "tuplevalue tuple<text,int>," //
					+ "mapoftuples map<text, frozen<tuple<address, list<text>, text>>>, " //
					+ "mapoftuplevalues map<text, frozen<tuple<text, int>>>, " //
					+ "mappedtuple frozen<tuple<address, list<text>, text>>, " //
					+ "mappedtuplewithvalue frozen<tuple<address, list<text>, text>>, " //
					+ "mappedtuples list<frozen<tuple<address, list<text>, text>>>, " //
					+ "PRIMARY KEY (id));";
			this.session.execute(ddl);
		} else {
			this.session.execute("TRUNCATE person;");
		}
	}

	@Test // DATACASS-651
	void shouldInsertRowWithTuple() {

		TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT);

		Person person = new Person();

		person.setId("foo");
		person.setTupleValue(tupleType.newValue("hello", 42));

		StatementFactory statementFactory = new StatementFactory(new UpdateMapper(converter));

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, WriteOptions.empty());

		this.session.execute(insert.build());

		ResultSet rows = this.session.execute("SELECT * FROM person");
		Row row = rows.one();

		assertThat(row.getObject("tuplevalue")).isEqualTo(person.getTupleValue());
	}

	@Test // DATACASS-523
	void shouldInsertRowWithComplexTuple() {

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

		StatementFactory statementFactory = new StatementFactory(new UpdateMapper(converter));

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, WriteOptions.empty());

		this.session.execute(insert.build());
	}

	@Test // DATACASS-523
	void shouldReadRowWithComplexTuple() {

		this.session.execute("INSERT INTO person (id,mappedtuple,mappedtuples) VALUES (" + "'foo'," //
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

	@Test // DATACASS-651
	void shouldInsertRowWithTupleMap() {

		Person person = new Person();
		person.setId("foo");

		MappedTuple tuple = new MappedTuple();
		tuple.setCurrency(Arrays.asList(Currency.getInstance("EUR"), Currency.getInstance("USD")));
		tuple.setName("bar");

		person.setMapOfTuples(Collections.singletonMap("foo", tuple));

		TupleType tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.INT);
		person.setMapOfTupleValues(Collections.singletonMap("mykey", tupleType.newValue("hello", 42)));

		StatementFactory statementFactory = new StatementFactory(new UpdateMapper(converter));

		StatementBuilder<RegularInsert> insert = statementFactory.insert(person, WriteOptions.empty());

		this.session.execute(insert.build());

		ResultSet rows = this.session.execute("SELECT * FROM person");
		Row row = rows.one();

		assertThat(row.getObject("mapoftuples")).isInstanceOf(Map.class);
		assertThat(row.getObject("mapoftuplevalues")).isInstanceOf(Map.class);
	}

	@Test // DATACASS-651
	void shouldReadRowWithMapOfTuples() {

		this.session.execute("INSERT INTO person (id,mapoftuples,mapoftuplevalues) VALUES "
				+ "('foo',{'foo':(NULL,['EUR','USD'],'bar')},{'mykey':('hello',42)});\n");

		ResultSet resultSet = this.session.execute("SELECT * FROM person;");

		Person person = this.converter.read(Person.class, resultSet.one());

		assertThat(person.getMapOfTupleValues()).hasSize(1);
		assertThat(person.getMapOfTupleValues().get("mykey").getString(0)).isEqualTo("hello");

		MappedTuple mappedTuple = person.getMapOfTuples().get("foo");
		assertThat(mappedTuple.getName()).isEqualTo("bar");
		assertThat(mappedTuple.getCurrency()).containsSequence(Currency.getInstance("EUR"), Currency.getInstance("USD"));
	}

	@Test // DATACASS-741
	void shouldReadTupleWithValue() {

		this.session.execute("INSERT INTO person (id,mappedtuplewithvalue) VALUES (" + "'foo'," //
				+ "({zip:'myzip'},['EUR','USD'],'bar'));\n");

		ResultSet resultSet = this.session.execute("SELECT * FROM person;");

		Person person = this.converter.read(Person.class, resultSet.one());

		MappedTupleWithValue mappedTuple = person.getMappedTupleWithValue();
		assertThat(mappedTuple.myName).isEqualTo("bar");
	}

	@Data
	@Table
	static class Person {

		@Id private String id;

		TupleValue tupleValue;
		MappedTuple mappedTuple;
		MappedTupleWithValue mappedTupleWithValue;
		List<MappedTuple> mappedTuples;
		Map<String, MappedTuple> mapOfTuples;
		Map<String, TupleValue> mapOfTupleValues;
	}

	@Data
	@Tuple
	static class MappedTuple {

		@Element(0) AddressUserType addressUserType;
		@Element(1) List<Currency> currency;
		@Element(2) String name;
	}

	@Tuple
	static class MappedTupleWithValue {

		private final @Element(0) AddressUserType addressUserType;
		private final @Element(1) List<Currency> currency;
		private final @Transient String myName;

		public MappedTupleWithValue(AddressUserType addressUserType, List<Currency> currency,
				@Value("#root.getString(2)") String myName) {
			this.addressUserType = addressUserType;
			this.currency = currency;
			this.myName = myName;
		}
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
