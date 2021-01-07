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
package org.springframework.data.cassandra.test.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Utility to mock a Cassandra {@link Row}.
 *
 * @author Mark Paluch
 */
public class RowMockUtil {

	/**
	 * Create a new {@link Row} mock using the given {@code columns}. Each column carries a name, value and data type so
	 * users of {@link Row} can use most of the methods.
	 */
	public static Row newRowMock(Column... columns) {

		Assert.notNull(columns, "Columns must not be null");

		Row mockRow = mock(Row.class);

		ColumnDefinitions mockColumnDefinitions = mock(ColumnDefinitions.class);

		when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);

		when(mockColumnDefinitions.contains(anyString())).thenAnswer(invocation -> Arrays.stream(columns)
				.anyMatch(column -> column.name.equalsIgnoreCase((String) invocation.getArguments()[0])));

		when(mockColumnDefinitions.firstIndexOf(anyString())).thenAnswer(invocation -> {

			int counter = 0;

			for (Column column : columns) {
				if (column.name.equalsIgnoreCase((String) invocation.getArguments()[0])) {
					return counter;
				}

				counter++;
			}

			return -1;
		});

		when(mockColumnDefinitions.contains(any(CqlIdentifier.class))).thenAnswer(invocation -> {

			for (Column column : columns) {
				if (column.name.equalsIgnoreCase(invocation.getArguments()[0].toString())) {
					return true;
				}
			}

			return false;
		});

		when(mockColumnDefinitions.get(anyInt())).thenAnswer(invocation ->
			new ColumnDefinition() {

				@Override
				public boolean isDetached() {
					return false;
				}

				@Override
				public void attach(@NonNull AttachmentPoint attachmentPoint) {

				}

				@NonNull
				@Override
				public CqlIdentifier getKeyspace() {
					return null;
				}

				@NonNull
				@Override
				public CqlIdentifier getTable() {
					return null;
				}

				@NonNull
				@Override
				public CqlIdentifier getName() {
					return CqlIdentifier.fromCql(columns[(Integer) invocation.getArguments()[0]].name);
				}

				@NonNull
				@Override
				public DataType getType() {
					return columns[(Integer) invocation.getArguments()[0]].type;
				}
			});

		when(mockRow.getBoolean(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getLocalDate(anyInt()))
				.thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getInstant(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getInetAddress(anyInt()))
				.thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getObject(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getString(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getLocalTime(anyInt()))
				.thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getTupleValue(anyInt()))
				.thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);
		when(mockRow.getUuid(anyInt())).thenAnswer(invocation -> columns[(Integer) invocation.getArguments()[0]].value);

		return mockRow;
	}

	/**
	 * Create a new {@link Column} to be used with {@link RowMockUtil#newRowMock(Column...)}.
	 *
	 * @param name must not be empty or {@code null}.
	 * @param value can be {@literal null}.
	 * @param type must not be {@literal null}.
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

		private Column(String name, Object value, DataType type) {
			this.name = name;
			this.value = value;
			this.type = type;
		}
	}
}
