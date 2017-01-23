/*
 * Copyright 2013-2017 the original author or authors
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;

/**
 * Simple constant holder for a {@link SimpleTypeHolder} enriched with Cassandra specific simple types.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author Antoine Toulme
 */
public class CassandraSimpleTypeHolder extends SimpleTypeHolder {

	public static final Set<Class<?>> CASSANDRA_SIMPLE_TYPES;

	private static final Map<Class<?>, DataType> classToDataType;
	private static final Map<DataType.Name, DataType> nameToDataType;

	static {

		CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;

		Map<Class<?>, Class<?>> primitiveWrappers = new HashMap<>(8);
		primitiveWrappers.put(Boolean.class, boolean.class);
		primitiveWrappers.put(Byte.class, byte.class);
		primitiveWrappers.put(Character.class, char.class);
		primitiveWrappers.put(Double.class, double.class);
		primitiveWrappers.put(Float.class, float.class);
		primitiveWrappers.put(Integer.class, int.class);
		primitiveWrappers.put(Long.class, long.class);
		primitiveWrappers.put(Short.class, short.class);

		Set<Class<?>> simpleTypes = getCassandraPrimitiveTypes(codecRegistry);
		simpleTypes.add(Number.class);
		simpleTypes.add(Row.class);
		simpleTypes.add(UDTValue.class);

		classToDataType = Collections.unmodifiableMap(classToDataType(primitiveWrappers, codecRegistry));
		nameToDataType = Collections.unmodifiableMap(nameToDataType());
		CASSANDRA_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
	}

	public static final SimpleTypeHolder HOLDER = new CassandraSimpleTypeHolder();

	/**
	 * @return the map between {@link Name} and {@link DataType}.
	 */
	private static Map<Name, DataType> nameToDataType() {

		Map<Name, DataType> nameToDataType = new HashMap<>(16);

		for (DataType dataType : DataType.allPrimitiveTypes()) {
			nameToDataType.put(dataType.getName(), dataType);
		}

		return nameToDataType;
	}

	/**
	 * @return the map between {@link Class} and {@link DataType}.
	 * @param primitiveWrappers map of primitive to wrapper type
	 * @param codecRegistry the Cassandra codec registry
	 */
	private static Map<Class<?>, DataType> classToDataType(Map<Class<?>, Class<?>> primitiveWrappers,
			CodecRegistry codecRegistry) {

		Map<Class<?>, DataType> classToDataType = new HashMap<>(16);

		for (DataType dataType : DataType.allPrimitiveTypes()) {

			Class<?> javaClass = codecRegistry.codecFor(dataType).getJavaType().getRawType();
			classToDataType.put(javaClass, dataType);

			Class<?> primitiveJavaClass = primitiveWrappers.get(javaClass);
			if (primitiveJavaClass != null) {
				classToDataType.put(primitiveJavaClass, dataType);
			}
		}

		// override String to text datatype as String is used multiple times
		classToDataType.put(String.class, DataType.text());

		// map Long to bigint as counter columns (last type aver multiple overrides)
		// are a special use case so map it to a more common type by
		// default
		classToDataType.put(Long.class, DataType.bigint());
		classToDataType.put(long.class, DataType.bigint());

		return classToDataType;
	}

	/**
	 * Returns a {@link Set} containing all Cassandra primitive types.
	 *
	 * @param codecRegistry the Cassandra codec registry
	 * @return the set of Cassandra primitive types.
	 */
	private static Set<Class<?>> getCassandraPrimitiveTypes(CodecRegistry codecRegistry) {

		return DataType.allPrimitiveTypes().stream() //
				.map(codecRegistry::codecFor) //
				.map(TypeCodec::getJavaType) //
				.map(TypeToken::getRawType) //
				.collect(Collectors.toSet());
	}

	/**
	 * Returns the {@link DataType} for a {@link DataType.Name}.
	 *
	 * @param name
	 * @return
	 */
	public static DataType getDataTypeFor(DataType.Name name) {
		return nameToDataType.get(name);
	}

	/**
	 * Returns the default {@link DataType} for a {@link Class}.
	 *
	 * @param javaClass
	 * @return
	 */
	public static DataType getDataTypeFor(Class<?> javaClass) {

		if (javaClass.isEnum()) {
			return DataType.varchar();
		}

		return classToDataType.get(javaClass);
	}

	public static DataType.Name[] getDataTypeNamesFrom(List<TypeInformation<?>> arguments) {

		DataType.Name[] array = new DataType.Name[arguments.size()];
		for (int i = 0; i != array.length; i++) {
			TypeInformation<?> typeInfo = arguments.get(i);
			DataType dataType = getDataTypeFor(typeInfo.getType());
			if (dataType == null) {
				throw new InvalidDataAccessApiUsageException(
						String.format("Did not find appropriate primitive DataType for type '%s'", typeInfo.getType()));
			}
			array[i] = dataType.getName();
		}
		return array;
	}

	public CassandraSimpleTypeHolder() {
		super(CASSANDRA_SIMPLE_TYPES, true);
	}
}
