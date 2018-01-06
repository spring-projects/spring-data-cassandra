/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.TypeMismatchDataAccessException;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Unit tests for {@link SingleColumnRowMapper}.
 *
 * @author Mark Paluch
 * @soundtrack Kos Vs Michael Buffer - Go For It All (Rubberboot Mix)
 */
@RunWith(MockitoJUnitRunner.class)
public class SingleColumnRowMapperUnitTests {

	@Mock private Row row;
	@Mock private ColumnDefinitions columnDefinitions;

	private SingleColumnRowMapper rowMapper;

	@Before
	public void before() throws Exception {
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
	}

	@Test // DATACASS-335
	public void getColumnValueWithType() {

		when(row.getDouble(2)).thenReturn(42d);

		rowMapper = new SingleColumnRowMapper();

		assertThat(rowMapper.getColumnValue(row, 2, Number.class)).isEqualTo(42d);
	}

	@Test // DATACASS-335
	public void getColumnValue() {

		when(row.getObject(2)).thenReturn(42d);

		rowMapper = new SingleColumnRowMapper();

		assertThat(rowMapper.getColumnValue(row, 2)).isEqualTo(42d);
	}

	@Test // DATACASS-335
	public void convertValueToRequiredTypeForNumber() {

		rowMapper = new SingleColumnRowMapper<Number>();

		assertThat(rowMapper.convertValueToRequiredType(1234, Integer.class)).isEqualTo(1234);
		assertThat(rowMapper.convertValueToRequiredType(1234.2, Integer.class)).isEqualTo(1234);
		assertThat(rowMapper.convertValueToRequiredType(1234.2, Double.class)).isEqualTo(1234.2);
	}

	@Test // DATACASS-335
	public void convertValueToRequiredTypeForString() {

		rowMapper = new SingleColumnRowMapper<Number>();

		assertThat(rowMapper.convertValueToRequiredType("1234", Integer.class)).isEqualTo(1234);
		assertThat(rowMapper.convertValueToRequiredType("1234.2", Double.class)).isEqualTo(1234.2);
	}

	@Test(expected = IllegalArgumentException.class) // DATACASS-335
	public void convertValueToRequiredTypeShouldFail() {

		rowMapper = new SingleColumnRowMapper<>();

		rowMapper.convertValueToRequiredType("1234", Object.class);
	}

	@Test // DATACASS-335
	public void mapRowSingleColumn() {

		when(columnDefinitions.size()).thenReturn(1);
		when(row.getInt(0)).thenReturn(42);

		rowMapper = SingleColumnRowMapper.newInstance(Integer.class);

		assertThat(rowMapper.mapRow(row, 2)).isEqualTo(42);
	}

	@Test // DATACASS-335
	public void mapRowSingleColumnNullValue() {

		when(columnDefinitions.size()).thenReturn(1);
		when(row.getObject(0)).thenReturn(null);

		rowMapper = SingleColumnRowMapper.newInstance(Object.class);

		assertThat(rowMapper.mapRow(row, 2)).isNull();
	}

	@Test(expected = TypeMismatchDataAccessException.class) // DATACASS-335
	public void mapRowSingleColumnWrongType() {

		when(columnDefinitions.size()).thenReturn(1);
		when(columnDefinitions.getType(0)).thenReturn(DataType.blob());
		when(row.getObject(0)).thenReturn("hello");

		rowMapper = SingleColumnRowMapper.newInstance(ColumnDefinitions.class);
		rowMapper.mapRow(row, 2);
	}

	@Test(expected = IncorrectResultSetColumnCountException.class) // DATACASS-335
	public void tooManyColumns() {

		when(columnDefinitions.size()).thenReturn(2);

		rowMapper = SingleColumnRowMapper.newInstance(ColumnDefinitions.class);
		rowMapper.mapRow(row, 1);
	}
}
