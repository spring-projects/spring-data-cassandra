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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

import com.datastax.driver.core.DataType;

/**
 * Simple constant holder for a {@link SimpleTypeHolder} enriched with Cassandra specific simple types.
 * 
 * @author Alex Shvid
 */
public class CassandraSimpleTypes {

	private static final Map<Class<?>, Class<?>> primitiveWrapperTypeMap = new HashMap<Class<?>, Class<?>>(8);

	private static final Map<Class<?>, DataType> javaClassToDataType = new HashMap<Class<?>, DataType>();
	
	private static final Map<DataType.Name, DataType> nameToDataType = new HashMap<DataType.Name, DataType>();
	
	static {
		
		primitiveWrapperTypeMap.put(Boolean.class, boolean.class);
		primitiveWrapperTypeMap.put(Byte.class, byte.class);
		primitiveWrapperTypeMap.put(Character.class, char.class);
		primitiveWrapperTypeMap.put(Double.class, double.class);
		primitiveWrapperTypeMap.put(Float.class, float.class);
		primitiveWrapperTypeMap.put(Integer.class, int.class);
		primitiveWrapperTypeMap.put(Long.class, long.class);
		primitiveWrapperTypeMap.put(Short.class, short.class);
		
		Set<Class<?>> simpleTypes = new HashSet<Class<?>>();
		for (DataType dataType : DataType.allPrimitiveTypes()) {
			simpleTypes.add(dataType.asJavaClass());
			Class<?> javaClass = dataType.asJavaClass();
			javaClassToDataType.put(javaClass, dataType);
			Class<?> primitiveJavaClass = primitiveWrapperTypeMap.get(javaClass);
			if (primitiveJavaClass != null) {
				javaClassToDataType.put(primitiveJavaClass, dataType);
			}
			nameToDataType.put(dataType.getName(), dataType);
		}
		javaClassToDataType.put(String.class, DataType.text());
		CASSANDRA_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
	}

	private static final Set<Class<?>> CASSANDRA_SIMPLE_TYPES;
	public static final SimpleTypeHolder HOLDER = new SimpleTypeHolder(CASSANDRA_SIMPLE_TYPES, true);

	private CassandraSimpleTypes() {
	}
	
	public static DataType resolvePrimitive(DataType.Name name) {
		return nameToDataType.get(name);
	}
	
	public static DataType autodetectPrimitive(Class<?> javaClass) {
		return javaClassToDataType.get(javaClass);
	}
	
	public static DataType.Name[] convertPrimitiveTypeArguments(List<TypeInformation<?>> arguments) {
		DataType.Name[] result = new DataType.Name[arguments.size()];
		for (int i = 0; i != result.length; ++i) {
			TypeInformation<?> type = arguments.get(i);
			DataType dataType = autodetectPrimitive(type.getType());
			if (dataType == null) {
				throw new InvalidDataAccessApiUsageException("not found appropriate primitive DataType for type = '" + type.getType());
			}
			result[i] = dataType.getName();
		}
		return result;
	}
	
}
