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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.UDTValue;

/**
 * @author Fabio Mendes <fabiojmendes@gmail.com> [Mar 17, 2015]
 *
 */
public abstract class AbstractColumnWriter {

	private SettableData<?> dataSetter;

	/**
	 * @param dataGetter
	 */
	public AbstractColumnWriter(SettableData<?> dataSetter) {
		this.dataSetter = dataSetter;
	}

	abstract protected DataType getDataType(int i);
	
	abstract protected int getColumnIndex(String name);
	
	/**
	 * Returns the row's column value.
	 */
	public void set(CqlIdentifier name, Object value) {
		set(name.toCql(), value);
	}

	/**
	 * Returns the row's column value.
	 */
	public void set(String name, Object value) {
		int indexOf = getColumnIndex(name);
		set(indexOf, value);
	}

	public void set(int i, Object value) {
		
		if (dataSetter == null) {
			throw new IllegalStateException("This data object is read-only");
		}
		
		if (value == null) {
			dataSetter.setToNull(i);
			return;
		}

		DataType type = getDataType(i);

		if (type.isCollection()) {

			List<DataType> collectionTypes = type.getTypeArguments();
			if (collectionTypes.size() == 2) {
				dataSetter.setMap(i, (Map<?,?>) value);
			}

			if (type.equals(DataType.list(collectionTypes.get(0)))) {
				dataSetter.setList(i, (List<?>) value);
			}

			if (type.equals(DataType.set(collectionTypes.get(0)))) {
				dataSetter.setSet(i, (Set<?>) value);
			}

			throw new IllegalStateException("Unknown Collection type encountered.  Valid collections are Set, List and Map.");
		} else if (type.equals(DataType.text()) || type.equals(DataType.ascii()) || type.equals(DataType.varchar())) {
			dataSetter.setString(i, (String) value);
		} else if (type.equals(DataType.cint())) {
			dataSetter.setInt(i, (Integer) value);
		} else if (type.equals(DataType.varint())) {
			dataSetter.setVarint(i, (BigInteger) value);
		} else if (type.equals(DataType.cdouble())) {
			dataSetter.setDouble(i, (Double) value);
		} else if (type.equals(DataType.bigint()) || type.equals(DataType.counter())) {
			dataSetter.setLong(i, (Long) value);
		} else if (type.equals(DataType.cfloat())) {
			dataSetter.setFloat(i, (Float) value);
		} else if (type.equals(DataType.decimal())) {
			dataSetter.setDecimal(i, (BigDecimal) value);
		} else if (type.equals(DataType.cboolean())) {
			dataSetter.setBool(i, (Boolean) value);
		} else if (type.equals(DataType.timestamp())) {
			dataSetter.setDate(i, (Date) value);
		} else if (type.equals(DataType.blob())) {
			dataSetter.setBytes(i, (ByteBuffer) value);
		} else if (type.equals(DataType.inet())) {
			dataSetter.setInet(i, (InetAddress) value);
		} else if (type.equals(DataType.uuid()) || type.equals(DataType.timeuuid())) {
			dataSetter.setUUID(i, (UUID) value);
		} else if (type.getName().equals(DataType.Name.UDT)) {
			dataSetter.setUDTValue(i, (UDTValue) value);
		} else {
			dataSetter.setBytesUnsafe(i, (ByteBuffer) value);
		}
	}
}
