/*
 * Copyright 2018-2025 the original author or authors.
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
import static org.springframework.data.cassandra.test.util.RowMockUtil.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Element;
import org.springframework.data.cassandra.core.mapping.Tuple;
import org.springframework.data.cassandra.test.util.RowMockUtil;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;

/**
 * Unit tests for mapped tuples through {@link MappingCassandraConverter}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MappingCassandraConverterMappedTupleUnitTests {

	private CassandraMappingContext mappingContext;

	private MappingCassandraConverter converter;

	private Row rowMock;

	@BeforeEach
	void setUp() {

		this.mappingContext = new CassandraMappingContext();
		this.converter = new MappingCassandraConverter(mappingContext);
		this.converter.afterPropertiesSet();
	}

	@Test // DATACASS-523
	void shouldReadMappedTupleValue() {

		BasicCassandraPersistentEntity<?> entity = this.mappingContext.getRequiredPersistentEntity(MappedTuple.class);

		CassandraColumnType type = converter.getColumnTypeResolver().resolve(entity.getTypeInformation());

		TupleValue value = ((TupleType) type.getDataType()).newValue("hello", 1);

		this.rowMock = RowMockUtil.newRowMock(column("name", "Jon Doe", DataTypes.TEXT),
				column("tuple", value, type.getDataType()));

		Person person = this.converter.read(Person.class, rowMock);

		assertThat(person).isNotNull();
		assertThat(person.name()).isEqualTo("Jon Doe");

		MappedTuple tuple = person.tuple();

		assertThat(tuple.name()).isEqualTo("hello");
		assertThat(tuple.position()).isEqualTo(1);
	}

	@Test // DATACASS-523
	void shouldWriteMappedTuple() {

		MappedTuple tuple = new MappedTuple("hello", 1);
		Person person = new Person("Jon Doe", tuple);

		Map<CqlIdentifier, Object> insert = new LinkedHashMap<>();

		this.converter.write(person, insert);

		TupleValue tupleValue = (TupleValue) insert.get(CqlIdentifier.fromCql("tuple"));
		assertThat(tupleValue.getFormattedContents()).contains("('hello',1)");
	}

	record Person(String name, MappedTuple tuple) {

	}

	@Tuple
	record MappedTuple(@Element(0) String name, @Element(1) int position) {

	}
}
