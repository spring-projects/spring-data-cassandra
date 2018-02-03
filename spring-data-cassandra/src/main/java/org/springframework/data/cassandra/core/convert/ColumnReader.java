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
package org.springframework.data.cassandra.core.convert;

import java.util.List;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;

/**
 * Helpful class to read a column's value from a row, with possible type conversion.
 *
 * @author Matthew T. Adams
 * @author Antoine Toulme
 * @author Mark Paluch
 */
public class ColumnReader {

	private final Row row;
	private final ColumnDefinitions columns;
	private final CodecRegistry codecRegistry;

	public ColumnReader(Row row) {

		this.row = row;
		this.columns = row.getColumnDefinitions();
		this.codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
	}

	/**
	 * Returns the row's column value.
	 */
	@Nullable
	public Object get(CqlIdentifier columnName) {
		return get(columnName.toCql());
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

		DataType type = columns.getType(columnIndex);

		if (type.isCollection()) {
			return getCollection(columnIndex, type);
		}

		if (Name.TUPLE.equals(type.getName())) {
			return row.getTupleValue(columnIndex);
		}

		if (Name.UDT.equals(type.getName())) {
			return row.getUDTValue(columnIndex);
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
		return get(columnName.toCql(), requestedType);
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

		List<DataType> collectionTypes = type.getTypeArguments();

		// List/Set
		if (collectionTypes.size() == 1) {

			DataType valueType = collectionTypes.get(0);

			TypeCodec<Object> typeCodec = codecRegistry.codecFor(valueType);

			if (type.equals(DataType.list(valueType))) {
				return row.getList(index, typeCodec.getJavaType().getRawType());
			}

			if (type.equals(DataType.set(valueType))) {
				return row.getSet(index, typeCodec.getJavaType().getRawType());
			}
		}

		// Map
		if (type.getName() == Name.MAP) {
			return row.getObject(index);
		}

		throw new IllegalStateException("Unknown Collection type encountered; valid collections are List, Set and Map.");
	}

	private int getColumnIndex(String columnName) {

		int index = columns.getIndexOf(columnName);

		Assert.isTrue(index > -1, String.format("Column [%s] does not exist in table", columnName));

		return index;
	}

	public Row getRow() {
		return row;
	}
}
