/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.convert;

import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;

/**
 * @author Fabio Mendes
 *
 */
public abstract class AbstractColumnReader {
	
	protected GettableData dataGetter;

	/**
	 * @param dataGetter
	 */
	public AbstractColumnReader(GettableData dataGetter) {
		this.dataGetter = dataGetter;
	}

	abstract protected DataType getDataType(int i);
	
	abstract protected int getColumnIndex(String name);
	
	/**
	 * Returns the row's column value.
	 */
	public Object get(CqlIdentifier name) {
		return get(name.toCql());
	}

	/**
	 * Returns the row's column value.
	 */
	public Object get(String name) {
		int indexOf = getColumnIndex(name);
		return get(indexOf);
	}

	public Object get(int i) {

		if (dataGetter.isNull(i)) {
			return null;
		}

		DataType type = getDataType(i);

		if (type.isCollection()) {

			List<DataType> collectionTypes = type.getTypeArguments();
			if (collectionTypes.size() == 2) {
				return dataGetter.getMap(i, collectionTypes.get(0).asJavaClass(), collectionTypes.get(1).asJavaClass());
			}

			if (type.equals(DataType.list(collectionTypes.get(0)))) {
				return dataGetter.getList(i, collectionTypes.get(0).asJavaClass());
			}

			if (type.equals(DataType.set(collectionTypes.get(0)))) {
				return dataGetter.getSet(i, collectionTypes.get(0).asJavaClass());
			}

			throw new IllegalStateException("Unknown Collection type encountered.  Valid collections are Set, List and Map.");
		}

		if (type.equals(DataType.text()) || type.equals(DataType.ascii()) || type.equals(DataType.varchar())) {
			return dataGetter.getString(i);
		}
		if (type.equals(DataType.cint())) {
			return new Integer(dataGetter.getInt(i));
		}
		if (type.equals(DataType.varint())) {
			return dataGetter.getVarint(i);
		}
		if (type.equals(DataType.cdouble())) {
			return new Double(dataGetter.getDouble(i));
		}
		if (type.equals(DataType.bigint()) || type.equals(DataType.counter())) {
			return new Long(dataGetter.getLong(i));
		}
		if (type.equals(DataType.cfloat())) {
			return new Float(dataGetter.getFloat(i));
		}
		if (type.equals(DataType.decimal())) {
			return dataGetter.getDecimal(i);
		}
		if (type.equals(DataType.cboolean())) {
			return new Boolean(dataGetter.getBool(i));
		}
		if (type.equals(DataType.timestamp())) {
			return dataGetter.getDate(i);
		}
		if (type.equals(DataType.blob())) {
			return dataGetter.getBytes(i);
		}
		if (type.equals(DataType.inet())) {
			return dataGetter.getInet(i);
		}
		if (type.equals(DataType.uuid()) || type.equals(DataType.timeuuid())) {
			return dataGetter.getUUID(i);
		}

		return dataGetter.getBytesUnsafe(i);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 * 
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	public <T> T get(CqlIdentifier name, Class<T> requestedType) {
		return get(getColumnIndex(name.toCql()), requestedType);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 * 
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	public <T> T get(String name, Class<T> requestedType) {
		return get(getColumnIndex(name), requestedType);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 * 
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(int i, Class<T> requestedType) {

		Object o = get(i);

		if (o == null) {
			return null;
		}

		return (T) o;
	}
}
