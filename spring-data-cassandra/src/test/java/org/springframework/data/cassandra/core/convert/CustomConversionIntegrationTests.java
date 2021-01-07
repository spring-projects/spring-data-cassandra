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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.convert.CustomConversions;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test suite for applying {@link CustomConversions} to {@link MappingCassandraConverter} and
 * {@link CassandraMappingContext}.
 *
 * @author Mark Paluch
 */
class CustomConversionIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraTemplate cassandraOperations;

	@BeforeEach
	void setUp() {

		MappingCassandraConverter converter = createConverter();
		cassandraOperations = new CassandraTemplate(session, converter);

		SchemaTestUtils.potentiallyCreateTableFor(Employee.class, cassandraOperations);
		SchemaTestUtils.truncate(Employee.class, cassandraOperations);
	}

	@Test // DATACASS-296
	void shouldInsertCustomConvertedObject() {

		Employee employee = new Employee();
		employee.setId("employee-id");
		employee.setPerson(new Person("Homer", "Simpson"));

		cassandraOperations.insert(employee);

		Row row = cassandraOperations.selectOne("SELECT id, person FROM employee", Row.class);

		assertThat(row.getString("id")).isEqualTo("employee-id");
		assertThat(row.getString("person")).contains("\"firstname\":\"Homer\"");
	}

	@Test // DATACASS-296
	void shouldUpdateCustomConvertedObject() {

		Employee employee = new Employee();
		employee.setId("employee-id");

		cassandraOperations.insert(employee);

		employee.setPerson(new Person("Homer", "Simpson"));
		cassandraOperations.update(employee);

		Row row = cassandraOperations.selectOne("SELECT id, person FROM employee", Row.class);

		assertThat(row.getString("id")).isEqualTo("employee-id");
		assertThat(row.getString("person")).contains("\"firstname\":\"Homer\"");
	}

	@Test // DATACASS-296
	void shouldInsertCustomConvertedObjectWithCollections() {

		Employee employee = new Employee();
		employee.setId("employee-id");
		cassandraOperations.insert(employee);

		employee.setFriends(Arrays.asList(new Person("Carl", "Carlson"), new Person("Lenny", "Leonard")));
		employee.setPeople(Collections.singleton(new Person("Apu", "Nahasapeemapetilon")));
		cassandraOperations.update(employee);

		Row row = cassandraOperations.selectOne("SELECT id, person, friends, people FROM employee", Row.class);

		assertThat(row.getObject("friends")).isInstanceOf(List.class);
		assertThat(row.getList("friends", String.class)).hasSize(2);

		assertThat(row.getObject("people")).isInstanceOf(Set.class);
		assertThat(row.getSet("people", String.class)).hasSize(1);
	}

	@Test // DATACASS-296
	void shouldUpdateCustomConvertedObjectWithCollections() {

		Employee employee = new Employee();
		employee.setId("employee-id");
		employee.setFriends(Arrays.asList(new Person("Carl", "Carlson"), new Person("Lenny", "Leonard")));
		employee.setPeople(Collections.singleton(new Person("Apu", "Nahasapeemapetilon")));

		cassandraOperations.insert(employee);

		Row row = cassandraOperations.selectOne("SELECT id, person, friends, people FROM employee", Row.class);

		assertThat(row.getObject("friends")).isInstanceOf(List.class);
		assertThat(row.getList("friends", String.class)).hasSize(2);

		assertThat(row.getObject("people")).isInstanceOf(Set.class);
		assertThat(row.getSet("people", String.class)).hasSize(1);
	}

	@Test // DATACASS-296
	void shouldLoadCustomConvertedObject() {

		cassandraOperations.getCqlOperations().execute(
				"INSERT INTO employee (id, person) VALUES('employee-id', '{\"firstname\":\"Homer\",\"lastname\":\"Simpson\"}')");

		Employee employee = cassandraOperations.selectOne("SELECT id, person FROM employee", Employee.class);

		assertThat(employee.getId()).isEqualTo("employee-id");
		assertThat(employee.getPerson()).isNotNull();
		assertThat(employee.getPerson().getFirstname()).isEqualTo("Homer");
		assertThat(employee.getPerson().getLastname()).isEqualTo("Simpson");
	}

	@Test // DATACASS-296
	void shouldLoadCustomConvertedWithCollectionsObject() {

		cassandraOperations.getCqlOperations().execute(
				"INSERT INTO employee (id, people) VALUES('employee-id', {'{\"firstname\":\"Apu\",\"lastname\":\"Nahasapeemapetilon\"}'})");

		Employee employee = cassandraOperations.selectOne("SELECT id, people FROM employee", Employee.class);

		assertThat(employee.getId()).isEqualTo("employee-id");
		assertThat(employee.getPeople()).isNotNull();

		assertThat(employee.getPeople()).extracting(Person::getFirstname).contains("Apu");
	}

	@Test // DATACASS-607
	void shouldApplyCustomReadConverterIfOnlyReadIsCustomized() {

		MappingCassandraConverter converter = createConverter(converters -> {
			converters.add(new PersonReadConverter());
		});

		cassandraOperations = new CassandraTemplate(session, converter);

		cassandraOperations.getCqlOperations().execute(
				"INSERT INTO employee (id, people) VALUES('employee-id', {'{\"firstname\":\"Apu\",\"lastname\":\"Nahasapeemapetilon\"}'})");

		Employee employee = cassandraOperations.selectOne("SELECT id, people FROM employee", Employee.class);

		assertThat(employee.getId()).isEqualTo("employee-id");
		assertThat(employee.getPeople()).isNotNull().hasSize(1);

		assertThat(employee.getPeople()).extracting(Person::getFirstname).contains("Apu");
	}

	private static MappingCassandraConverter createConverter() {

		return createConverter(converters -> {
			converters.add(new PersonReadConverter());
			converters.add(new PersonWriteConverter());

		});
	}

	private static MappingCassandraConverter createConverter(Consumer<List<Converter<?, ?>>> converterCustomizer) {

		List<Converter<?, ?>> converters = new ArrayList<>();

		converterCustomizer.accept(converters);

		CustomConversions customConversions = new CassandraCustomConversions(converters);

		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.setCustomConversions(customConversions);
		mappingContext.afterPropertiesSet();

		MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);
		converter.setCustomConversions(customConversions);
		converter.afterPropertiesSet();
		return converter;
	}

	@Data
	@Table
	static class Employee {

		@Id String id;

		Person person;
		List<Person> friends;
		Set<Person> people;
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Person {

		String firstname;
		String lastname;
	}

	/**
	 * @author Mark Paluch
	 */
	private static class PersonReadConverter implements Converter<String, Person> {

		public Person convert(String source) {

			if (StringUtils.hasText(source)) {
				try {
					return new ObjectMapper().readValue(source, Person.class);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}

			return null;
		}
	}

	/**
	 * @author Mark Paluch
	 */
	private static class PersonWriteConverter implements Converter<Person, String> {

		public String convert(Person source) {

			try {
				return new ObjectMapper().writeValueAsString(source);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
