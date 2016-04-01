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
package org.springframework.data.cassandra.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.CodecRegistry;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

import com.datastax.driver.core.DataType;

/**
 * Simple constant holder for a {@link SimpleTypeHolder} enriched with Cassandra specific simple types.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class CassandraSimpleTypeHolder extends SimpleTypeHolder {

	public static final Set<Class<?>> CASSANDRA_SIMPLE_TYPES;

	private static final Map<Class<?>, Class<?>> primitiveTypesByWrapperType = new HashMap<Class<?>, Class<?>>(8);

	private static final Map<Class<?>, DataType> dataTypesByJavaClass = new HashMap<Class<?>, DataType>();

	private static final Map<DataType.Name, DataType> dataTypesByDataTypeName = new HashMap<DataType.Name, DataType>();
	static Class<?>  parseRawType(DataType dataType){
		return CodecRegistry.DEFAULT_INSTANCE.codecFor(dataType).getJavaType().getRawType();
	}
	static {

		primitiveTypesByWrapperType.put(Boolean.class, boolean.class);
		primitiveTypesByWrapperType.put(Byte.class, byte.class);
		primitiveTypesByWrapperType.put(Character.class, char.class);
		primitiveTypesByWrapperType.put(Double.class, double.class);
		primitiveTypesByWrapperType.put(Float.class, float.class);
		primitiveTypesByWrapperType.put(Integer.class, int.class);
		primitiveTypesByWrapperType.put(Long.class, long.class);
		primitiveTypesByWrapperType.put(Short.class, short.class);

		Set<Class<?>> simpleTypes = new HashSet<Class<?>>();

		for (DataType dataType : DataType.allPrimitiveTypes()) {

			Class<?> javaClass =parseRawType( dataType);
			simpleTypes.add(javaClass);

			dataTypesByJavaClass.put(javaClass, dataType);

			Class<?> primitiveJavaClass = primitiveTypesByWrapperType.get(javaClass);
			if (primitiveJavaClass != null) {
				dataTypesByJavaClass.put(primitiveJavaClass, dataType);
			}

			dataTypesByDataTypeName.put(dataType.getName(), dataType);
		}

		dataTypesByJavaClass.put(String.class, DataType.text());

		CASSANDRA_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
	}

	public static DataType getDataTypeFor(DataType.Name name) {
		return dataTypesByDataTypeName.get(name);
	}

	public static DataType getDataTypeFor(Class<?> javaClass) {
		return dataTypesByJavaClass.get(javaClass);
	}

	public static DataType.Name[] getDataTypeNamesFrom(List<TypeInformation<?>> arguments) {
		DataType.Name[] array = new DataType.Name[arguments.size()];
		for (int i = 0; i != array.length; i++) {
			TypeInformation<?> typeInfo = arguments.get(i);
			DataType dataType = getDataTypeFor(typeInfo.getType());
			if (dataType == null) {
				throw new InvalidDataAccessApiUsageException("not found appropriate primitive DataType for type = '"
						+ typeInfo.getType());
			}
			array[i] = dataType.getName();
		}
		return array;
	}

	public CassandraSimpleTypeHolder() {
		super(CASSANDRA_SIMPLE_TYPES, true);
	}
}
