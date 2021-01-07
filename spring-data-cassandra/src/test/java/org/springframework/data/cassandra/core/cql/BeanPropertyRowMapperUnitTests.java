/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.dao.InvalidDataAccessApiUsageException;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Unit tests for {@link BeanPropertyRowMapper}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BeanPropertyRowMapperUnitTests {

	@Mock Row row;

	@Test // DATACASS-810
	void createBeanFromRow() {

		ColumnDefinitions definitions = forColumns("firstname", "age");
		when(row.getColumnDefinitions()).thenReturn(definitions);
		when(row.get(0, String.class)).thenReturn("Walter");
		when(row.get(1, int.class)).thenReturn(42);

		BeanPropertyRowMapper<Person> rowMapper = new BeanPropertyRowMapper<>(Person.class);

		Person person = rowMapper.mapRow(row, 0);

		assertThat(person.firstname).isEqualTo("Walter");
		assertThat(person.age).isEqualTo(42);
	}

	@Test // DATACASS-810
	void createBeanFromRowWithNullDefault() {

		ColumnDefinitions definitions = forColumns("firstname", "age");
		when(row.getColumnDefinitions()).thenReturn(definitions);
		when(row.get(0, String.class)).thenReturn("Walter");

		BeanPropertyRowMapper<Person> rowMapper = new BeanPropertyRowMapper<>(Person.class);
		rowMapper.setPrimitivesDefaultedForNullValue(true);

		Person person = rowMapper.mapRow(row, 0);

		assertThat(person.firstname).isEqualTo("Walter");
		assertThat(person.age).isEqualTo(0);
	}

	@Test // DATACASS-810
	void shouldRefusePartiallyPopulatedResult() {

		ColumnDefinitions definitions = forColumns("age");
		when(row.getColumnDefinitions()).thenReturn(definitions);
		when(row.get(0, int.class)).thenReturn(42);

		BeanPropertyRowMapper<Person> rowMapper = new BeanPropertyRowMapper<>(Person.class);
		rowMapper.setCheckFullyPopulated(true);

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> rowMapper.mapRow(row, 0));
	}

	static class Person {

		String firstname;
		int age;

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}

	private static ColumnDefinitions forColumns(String... columns) {

		ColumnDefinitions definitions = mock(ColumnDefinitions.class);

		int index = 0;
		for (String column : columns) {

			ColumnDefinition columnDefinition = mock(ColumnDefinition.class);
			when(columnDefinition.getName()).thenReturn(CqlIdentifier.fromInternal(column));

			when(definitions.get(index++)).thenReturn(columnDefinition);
		}

		when(definitions.size()).thenReturn(index);

		return definitions;
	}

}
