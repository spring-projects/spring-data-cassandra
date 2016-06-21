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
package org.springframework.data.cassandra.test.integration.mapping.customconversion;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CustomConversions;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.repository.querymethods.datekey.DateThingRepo;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Test suite for applying {@link CustomConversions} to
 * {@link org.springframework.data.cassandra.mapping.BasicCassandraMappingContext}.
 * 
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CustomConversionTests {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = DateThingRepo.class)
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Employee.class.getPackage().getName() };
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}

		@Override
		public CustomConversions customConversions() {

			List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();
			converters.add(new PersonReadConverter());
			converters.add(new PersonWriteConverter());

			return new CustomConversions(converters);
		}
	}

	@Autowired CassandraOperations cassandraOperations;

	@Before
	public void setUp() {
		cassandraOperations.deleteAll(Employee.class);
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldInsertCustomConvertedObject() {

		Employee employee = new Employee();
		employee.setId("employee-id");
		employee.setPerson(new Person("Homer", "Simpson"));

		cassandraOperations.insert(employee);

		Row row = cassandraOperations.selectOne(QueryBuilder.select("id", "person").from("employee"), Row.class);

		assertThat(row.getString("id"), is(equalTo("employee-id")));
		assertThat(row.getString("person"), containsString("\"firstname\":\"Homer\""));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldUpdateCustomConvertedObject() {

		Employee employee = new Employee();
		employee.setId("employee-id");

		cassandraOperations.insert(employee);

		employee.setPerson(new Person("Homer", "Simpson"));
		cassandraOperations.update(employee);

		Row row = cassandraOperations.selectOne(QueryBuilder.select("id", "person").from("employee"), Row.class);

		assertThat(row.getString("id"), is(equalTo("employee-id")));
		assertThat(row.getString("person"), containsString("\"firstname\":\"Homer\""));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldInsertCustomConvertedObjectWithCollections() {

		Employee employee = new Employee();
		employee.setId("employee-id");
		cassandraOperations.insert(employee);

		employee.setFriends(Arrays.asList(new Person("Carl", "Carlson"), new Person("Lenny", "Leonard")));
		employee.setPeople(Collections.singleton(new Person("Apu", "Nahasapeemapetilon")));
		cassandraOperations.update(employee);

		Row row = cassandraOperations.selectOne(QueryBuilder.select("id", "person", "friends", "people").from("employee"),
				Row.class);

		assertThat(row.getObject("friends"), is(instanceOf(List.class)));
		assertThat(row.getList("friends", String.class), hasSize(2));

		assertThat(row.getObject("people"), is(instanceOf(Set.class)));
		assertThat(row.getSet("people", String.class), hasSize(1));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldUpdateCustomConvertedObjectWithCollections() {

		Employee employee = new Employee();
		employee.setId("employee-id");
		employee.setFriends(Arrays.asList(new Person("Carl", "Carlson"), new Person("Lenny", "Leonard")));
		employee.setPeople(Collections.singleton(new Person("Apu", "Nahasapeemapetilon")));

		cassandraOperations.insert(employee);

		Row row = cassandraOperations.selectOne(QueryBuilder.select("id", "person", "friends", "people").from("employee"),
				Row.class);

		assertThat(row.getObject("friends"), is(instanceOf(List.class)));
		assertThat(row.getList("friends", String.class), hasSize(2));

		assertThat(row.getObject("people"), is(instanceOf(Set.class)));
		assertThat(row.getSet("people", String.class), hasSize(1));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldLoadCustomConvertedObject() {

		cassandraOperations.execute(QueryBuilder.insertInto("employee").value("id", "employee-id").value("person",
				"{\"firstname\":\"Homer\",\"lastname\":\"Simpson\"}"));

		Employee employee = cassandraOperations.selectOne(QueryBuilder.select("id", "person").from("employee"),
				Employee.class);

		assertThat(employee.getId(), is(equalTo("employee-id")));
		assertThat(employee.getPerson(), is(notNullValue()));
		assertThat(employee.getPerson().getFirstname(), is(equalTo("Homer")));
		assertThat(employee.getPerson().getLastname(), is(equalTo("Simpson")));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void shouldLoadCustomConvertedWithCollectionsObject() {

		cassandraOperations.execute(QueryBuilder.insertInto("employee").value("id", "employee-id").value("people",
				Collections.singleton("{\"firstname\":\"Apu\",\"lastname\":\"Nahasapeemapetilon\"}")));

		Employee employee = cassandraOperations.selectOne(QueryBuilder.select("id", "people").from("employee"),
				Employee.class);

		assertThat(employee.getId(), is(equalTo("employee-id")));
		assertThat(employee.getPeople(), is(notNullValue()));

		Person apu = employee.getPeople().iterator().next();
		assertThat(apu.getFirstname(), is(equalTo("Apu")));
	}

	/**
	 * @see DATACASS-296
	 */
	@Test
	public void dummy() {

		cassandraOperations.execute(QueryBuilder.insertInto("employee").value("id", "employee-id"));

		cassandraOperations
				.execute(QueryBuilder.update("employee").where(QueryBuilder.eq("id", "employee-id")).with(QueryBuilder
						.set("people", Collections.singleton("{\"firstname\":\"Apu\",\"lastname\":\"Nahasapeemapetilon\"}"))));
	}

	/**
	 * @author Mark Paluch
	 */
	@Data
	@Table
	static class Employee {

		@Id String id;

		Person person;
		List<Person> friends;
		Set<Person> people;
	}

	/**
	 * @author Mark Paluch
	 */
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
	static class PersonReadConverter implements Converter<String, Person> {

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
	static class PersonWriteConverter implements Converter<Person, String> {

		public String convert(Person source) {

			try {
				return new ObjectMapper().writeValueAsString(source);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
