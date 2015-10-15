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

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

/**
 * Helpful class to read a user defined type value from a row, with possible type conversion.
 * 
 * @author Fabio Mendes
 */
public class UDTValueReader extends AbstractColumnReader {

	protected UDTValue udtValue;
	protected UserType udtType;
	private List<String> fieldNames;

	public UDTValueReader(UDTValue value) {
		super(value);
		this.udtValue = value;
		this.udtType = value.getType();
		this.fieldNames = new ArrayList<String>(udtType.getFieldNames());
	}

	@Override
	protected DataType getDataType(int i) {
		return udtType.getFieldType(fieldNames.get(i));
	}

	protected int getColumnIndex(String name) {
		int indexOf = fieldNames.indexOf(name);
		if (indexOf == -1) {
			throw new IllegalArgumentException("Column does not exist in Cassandra table: " + name);
		}
		return indexOf;
	}

	public UDTValue getUDTValue() {
		return udtValue;
	}
}
