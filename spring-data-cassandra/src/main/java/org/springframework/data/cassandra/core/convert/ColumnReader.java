/*
 * Copyright 2016-2017 the original author or authors.
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
	public Object get(CqlIdentifier name) {
		return get(name.toCql());
	}

	/**
	 * Returns the row's column value.
	 */
	@Nullable
	public Object get(String name) {
		return get(getColumnIndex(name));
	}

	/**
	 * Read data from a Column using the {@code index}.
	 *
	 * @param index
	 * @return
	 */
	@Nullable
	public Object get(int index) {

		if (row.isNull(index)) {
			return null;
		}

		DataType type = columns.getType(index);

		if (type.isCollection()) {
			return getCollection(index, type);
		}

		if (Name.TUPLE.equals(type.getName())) {
			return row.getTupleValue(index);
		}

		if (Name.UDT.equals(type.getName())) {
			return row.getUDTValue(index);
		}

		return row.getObject(index);
	}

	@Nullable
	private Object getCollection(int i, DataType type) {

		List<DataType> collectionTypes = type.getTypeArguments();

		// List/Set
		if (collectionTypes.size() == 1) {

			DataType valueType = collectionTypes.get(0);
			TypeCodec<Object> typeCodec = codecRegistry.codecFor(valueType);
			if (type.equals(DataType.list(valueType))) {
				return row.getList(i, typeCodec.getJavaType().getRawType());
			}

			if (type.equals(DataType.set(valueType))) {
				return row.getSet(i, typeCodec.getJavaType().getRawType());
			}
		}

		// Map
		if (collectionTypes.size() == 2) {

			DataType keyType = collectionTypes.get(0);
			TypeCodec<Object> keyTypeCodec = codecRegistry.codecFor(keyType);

			DataType valueType = collectionTypes.get(1);
			TypeCodec<Object> valueTypeCodec = codecRegistry.codecFor(valueType);
			return row.getMap(i, keyTypeCodec.getJavaType().getRawType(), valueTypeCodec.getJavaType().getRawType());
		}

		throw new IllegalStateException("Unknown Collection type encountered. Valid collections are Set, List and Map.");
	}

	public Row getRow() {
		return row;
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 *
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	@Nullable
	public <T> T get(CqlIdentifier name, Class<T> requestedType) {
		return get(getColumnIndex(name.toCql()), requestedType);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 *
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	@Nullable
	public <T> T get(String name, Class<T> requestedType) {
		return get(columns.getIndexOf(name), requestedType);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 *
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	@Nullable
	public <T> T get(int i, Class<T> requestedType) {

		Object o = get(i);

		if (o == null) {
			return null;
		}

		return requestedType.cast(o);
	}

	private int getColumnIndex(String name) {

		int indexOf = columns.getIndexOf(name);
		if (indexOf == -1) {
			throw new IllegalArgumentException("Column does not exist in Cassandra table: " + name);
		}
		return indexOf;
	}
}
