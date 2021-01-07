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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.dao.TypeMismatchDataAccessException;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit tests for {@link SingleColumnRowMapper}.
 *
 * @author Mark Paluch
 * @soundtrack Kos Vs Michael Buffer - Go For It All (Rubberboot Mix)
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SingleColumnRowMapperUnitTests {

	@Mock Row row;
	@Mock ColumnDefinition columnDefinition;
	@Mock ColumnDefinitions columnDefinitions;

	private SingleColumnRowMapper rowMapper;

	@BeforeEach
	void before() throws Exception {
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
	}

	@Test // DATACASS-335
	void getColumnValueWithType() {

		when(row.getDouble(2)).thenReturn(42d);

		rowMapper = new SingleColumnRowMapper();

		assertThat(rowMapper.getColumnValue(row, 2, Number.class)).isEqualTo(42d);
	}

	@Test // DATACASS-335
	void getColumnValue() {

		when(row.getObject(2)).thenReturn(42d);

		rowMapper = new SingleColumnRowMapper();

		assertThat(rowMapper.getColumnValue(row, 2)).isEqualTo(42d);
	}

	@Test // DATACASS-335
	void convertValueToRequiredTypeForNumber() {

		rowMapper = new SingleColumnRowMapper<Number>();

		assertThat(rowMapper.convertValueToRequiredType(1234, Integer.class)).isEqualTo(1234);
		assertThat(rowMapper.convertValueToRequiredType(1234.2, Integer.class)).isEqualTo(1234);
		assertThat(rowMapper.convertValueToRequiredType(1234.2, Double.class)).isEqualTo(1234.2);
	}

	@Test // DATACASS-335
	void convertValueToRequiredTypeForString() {

		rowMapper = new SingleColumnRowMapper<Number>();

		assertThat(rowMapper.convertValueToRequiredType("1234", Integer.class)).isEqualTo(1234);
		assertThat(rowMapper.convertValueToRequiredType("1234.2", Double.class)).isEqualTo(1234.2);
	}

	@Test // DATACASS-335
	void convertValueToRequiredTypeShouldFail() {

		rowMapper = new SingleColumnRowMapper<>();

		assertThatIllegalArgumentException().isThrownBy(() -> rowMapper.convertValueToRequiredType("1234", Object.class));
	}

	@Test // DATACASS-335
	void mapRowSingleColumn() {

		when(columnDefinitions.size()).thenReturn(1);
		when(row.getInt(0)).thenReturn(42);

		rowMapper = SingleColumnRowMapper.newInstance(Integer.class);

		assertThat(rowMapper.mapRow(row, 2)).isEqualTo(42);
	}

	@Test // DATACASS-335
	void mapRowSingleColumnNullValue() {

		when(columnDefinitions.size()).thenReturn(1);
		when(row.getObject(0)).thenReturn(null);

		rowMapper = SingleColumnRowMapper.newInstance(Object.class);

		assertThat(rowMapper.mapRow(row, 2)).isNull();
	}

	@Test // DATACASS-335
	void mapRowSingleColumnWrongType() {

		when(columnDefinitions.size()).thenReturn(1);
		when(columnDefinitions.get(0)).thenReturn(columnDefinition);

		when(row.getObject(0)).thenReturn("hello");

		rowMapper = SingleColumnRowMapper.newInstance(ColumnDefinitions.class);

		assertThatExceptionOfType(TypeMismatchDataAccessException.class).isThrownBy(() -> rowMapper.mapRow(row, 2));
	}

	@Test // DATACASS-335
	void tooManyColumns() {

		when(columnDefinitions.size()).thenReturn(2);

		rowMapper = SingleColumnRowMapper.newInstance(ColumnDefinitions.class);

		assertThatExceptionOfType(IncorrectResultSetColumnCountException.class).isThrownBy(() -> rowMapper.mapRow(row, 1));
	}
}
