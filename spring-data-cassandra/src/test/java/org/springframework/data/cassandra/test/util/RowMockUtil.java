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
package org.springframework.data.cassandra.test.util;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.springframework.util.Assert;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Utility to mock a Cassandra {@link Row}.
 *
 * @author Mark Paluch
 */
public class RowMockUtil {

	/**
	 * Create a new {@link Row} mock using the given {@code columns}. Each column carries a name, value and data type so
	 * users of {@link Row} can use most of the methods.
	 *
	 * @param columns
	 * @return
	 */
	public static Row newRowMock(Column... columns) {

		Assert.notNull(columns, "Columns must not be null");

		Row mockRow = mock(Row.class);

		ColumnDefinitions mockColumnDefinitions = mock(ColumnDefinitions.class);

		when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);

		when(mockColumnDefinitions.contains(anyString())).thenAnswer(invocation -> Arrays.stream(columns)
				.anyMatch(column -> column.name.equalsIgnoreCase((String) invocation.getArguments()[0])));

		when(mockColumnDefinitions.getIndexOf(anyString())).thenAnswer(invocation -> {

			int counter = 0;

			for (Column column : columns) {
				if (column.name.equalsIgnoreCase((String) invocation.getArguments()[0])) {
					return counter;
				}

				counter++;
			}

			return -1;
		});

		when(mockColumnDefinitions.getType(anyInt()))
				.thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].type);

		when(mockRow.getBool(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getDate(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getInet(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getObject(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getString(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getTimestamp(anyInt()))
				.thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getTupleValue(anyInt()))
			.thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getUUID(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);

		return mockRow;
	}

	/**
	 * Create a new {@link Column} to be used with {@link RowMockUtil#newRowMock(Column...)}.
	 *
	 * @param name must not be empty or {@link null}.
	 * @param value can be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public static Column column(String name, Object value, DataType type) {

		Assert.hasText(name, "Name must not be empty");
		Assert.notNull(type, "DataType must not be null");

		return new Column(name, value, type);
	}

	public static class Column {

		private final String name;
		private final Object value;
		private final DataType type;

		Column(String name, Object value, DataType type) {
			this.name = name;
			this.value = value;
			this.type = type;
		}
	}
}
