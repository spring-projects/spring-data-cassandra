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
import static org.springframework.data.cassandra.test.util.RowMockUtil.*;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Element;
import org.springframework.data.cassandra.core.mapping.Tuple;
import org.springframework.data.cassandra.test.util.RowMockUtil;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Unit tests for mapped tuples through {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class MappingCassandraConverterMappedTupleUnitTests {

	@Rule public final ExpectedException expectedException = ExpectedException.none();

	Row rowMock;

	CassandraMappingContext mappingContext;
	MappingCassandraConverter mappingCassandraConverter;

	@Before
	public void setUp() {

		mappingContext = new CassandraMappingContext();

		mappingCassandraConverter = new MappingCassandraConverter(mappingContext);
		mappingCassandraConverter.afterPropertiesSet();
	}

	@Test // DATACASS-523
	public void shouldReadMappedTupleValue() {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(MappedTuple.class);

		TupleValue value = entity.getTupleType().newValue("hello", 1);

		rowMock = RowMockUtil.newRowMock(column("tuple", value, entity.getTupleType()));

		Person person = mappingCassandraConverter.read(Person.class, rowMock);

		MappedTuple tuple = person.getTuple();

		assertThat(tuple.getName()).isEqualTo("hello");
		assertThat(tuple.getPosition()).isEqualTo(1);
	}

	@Test // DATACASS-523
	public void shouldWriteMappedTuple() {

		MappedTuple tuple = new MappedTuple("hello", 1);
		Person person = new Person(tuple);

		Insert insert = QueryBuilder.insertInto("table");

		mappingCassandraConverter.write(person, insert);

		assertThat(insert.toString()).contains("VALUES (('hello',1))");
	}

	@Data
	@AllArgsConstructor
	private static class Person {
		MappedTuple tuple;
	}

	@Tuple
	@Data
	@AllArgsConstructor
	private static class MappedTuple {

		@Element(0) String name;
		@Element(1) int position;
	}

}
