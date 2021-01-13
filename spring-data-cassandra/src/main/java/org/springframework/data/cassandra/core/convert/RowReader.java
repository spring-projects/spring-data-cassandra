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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Helpful class to read a column's value from a row, with possible type conversion.
 *
 * @author Mark Paluch
 * @author Frank Spitulski
 * @since 3.0
 */
class RowReader {

	private final com.datastax.oss.driver.api.core.cql.Row row;

	private final CodecRegistry codecRegistry;

	private final ColumnDefinitions columns;

	public RowReader(Row row) {

		this.row = row;
		this.codecRegistry = row.codecRegistry();
		this.columns = row.getColumnDefinitions();
	}

	/**
	 * Returns the row's column value.
	 */
	@Nullable
	public Object get(CqlIdentifier columnName) {
		return get(columnName.toString());
	}

	/**
	 * Returns the row's column value.
	 */
	@Nullable
	public Object get(String columnName) {
		return get(getColumnIndex(columnName));
	}

	/**
	 * Read data from Column at the given {@code index}.
	 *
	 * @param columnIndex {@link Integer#TYPE index} of the Column.
	 * @return the value of the Column in at index in the Row, or {@literal null} if the Column contains no value.
	 */
	@Nullable
	public Object get(int columnIndex) {

		if (row.isNull(columnIndex)) {
			return null;
		}

		ColumnDefinition type = columns.get(columnIndex);

		if (type.getType() instanceof ListType || type.getType() instanceof SetType || type.getType() instanceof MapType) {
			return getCollection(columnIndex, type.getType());
		}

		if (type instanceof TupleType) {
			return row.getTupleValue(columnIndex);
		}

		if (type instanceof UserDefinedType) {
			return row.getUdtValue(columnIndex);
		}

		return row.getObject(columnIndex);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 *
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	@Nullable
	public <T> T get(CqlIdentifier columnName, Class<T> requestedType) {
		return get(columnName.toString(), requestedType);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 *
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	@Nullable
	public <T> T get(String columnName, Class<T> requestedType) {
		return get(getColumnIndex(columnName), requestedType);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 *
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	@Nullable
	public <T> T get(int columnIndex, Class<T> requestedType) {

		Object value = get(columnIndex);

		return requestedType.cast(value);
	}

	@Nullable
	private Object getCollection(int index, DataType type) {

		if (type instanceof ListType) {

			ListType listType = (ListType) type;
			DataType valueType = listType.getElementType();
			TypeCodec<Object> typeCodec = codecRegistry.codecFor(valueType);

			return row.getList(index, typeCodec.getJavaType().getRawType());
		}

		if (type instanceof SetType) {

			SetType setType = (SetType) type;
			DataType valueType = setType.getElementType();
			TypeCodec<Object> typeCodec = codecRegistry.codecFor(valueType);

			return row.getSet(index, typeCodec.getJavaType().getRawType());
		}

		// Map
		if (type instanceof MapType) {

			return row.getObject(index);
		}

		throw new IllegalStateException("Unknown Collection type encountered; valid collections are List, Set and Map.");
	}

	private int getColumnIndex(String columnName) {

		int index = columns.firstIndexOf(columnName);

		Assert.isTrue(index > -1, () -> String.format("Column [%s] does not exist in table", columnName));

		return index;
	}

	public Row getRow() {
		return row;
	}

	public boolean contains(CqlIdentifier columnName) {
		return row.getColumnDefinitions().contains(columnName);
	}
}
