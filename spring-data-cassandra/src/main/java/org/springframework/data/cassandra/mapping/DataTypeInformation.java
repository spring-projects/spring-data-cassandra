/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import com.datastax.driver.core.DataType;

/**
 * Uses to transfer DataType and attributes for the property.
 * 
 * @author Alex Shvid
 */
public class DataTypeInformation {

	public static DataType.Name[] EMPTY_ATTRIBUTES = {};

	private DataType.Name typeName;
	private DataType.Name[] typeAttributes;

	public DataTypeInformation(DataType.Name typeName) {
		this(typeName, EMPTY_ATTRIBUTES);
	}

	public DataTypeInformation(DataType.Name typeName, DataType.Name[] typeAttributes) {
		this.typeName = typeName;
		this.typeAttributes = typeAttributes;
	}

	public DataType.Name getTypeName() {
		return typeName;
	}

	public void setTypeName(DataType.Name typeName) {
		this.typeName = typeName;
	}

	public DataType.Name[] getTypeAttributes() {
		return typeAttributes;
	}

	public void setTypeAttributes(DataType.Name[] typeAttributes) {
		this.typeAttributes = typeAttributes;
	}

	public String toCQL() {
		if (typeAttributes.length == 0) {
			return typeName.name();
		} else {
			StringBuilder str = new StringBuilder();
			str.append(typeName.name());
			str.append('<');
			for (int i = 0; i != typeAttributes.length; ++i) {
				if (i != 0) {
					str.append(',');
				}
				str.append(typeAttributes[i].name());
			}
			str.append('>');
			return str.toString();
		}
	}
}
