/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.lang.Nullable;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
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

	/**
	 * Set of Cassandra simple types.
	 */
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
		simpleTypes.add(TupleValue.class);
		simpleTypes.add(UDTValue.class);

		classToDataType = Collections.unmodifiableMap(classToDataType(codecRegistry, primitiveWrappers));
		nameToDataType = Collections.unmodifiableMap(nameToDataType());
		CASSANDRA_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
	}

	public static final SimpleTypeHolder HOLDER = new CassandraSimpleTypeHolder();

	/**
	 * Create a new {@link CassandraSimpleTypeHolder} instance.
	 */
	private CassandraSimpleTypeHolder() {
		super(CASSANDRA_SIMPLE_TYPES, true);
	}

	/**
	 * @return the map between {@link Class} and {@link DataType}.
	 * @param codecRegistry the Cassandra codec registry.
	 * @param primitiveWrappers map of primitive to wrapper type.
	 */
	private static Map<Class<?>, DataType> classToDataType(CodecRegistry codecRegistry,
			Map<Class<?>, Class<?>> primitiveWrappers) {

		Map<Class<?>, DataType> classToDataType = new HashMap<>(16);

		DataType.allPrimitiveTypes().forEach(dataType -> {

			Class<?> javaType = codecRegistry.codecFor(dataType).getJavaType().getRawType();

			classToDataType.put(javaType, dataType);

			Optional.ofNullable(primitiveWrappers.get(javaType))
					.ifPresent(primitiveType -> classToDataType.put(primitiveType, dataType));
		});

		// override String to text DataType as String is used multiple times
		classToDataType.put(String.class, DataType.text());

		// map Long to bigint as counter columns (last type aver multiple overrides) are a special use case
		// so map it to a more common type by default
		classToDataType.put(Long.class, DataType.bigint());
		classToDataType.put(long.class, DataType.bigint());

		// override UUID to timeuuid as regular uuid as the favored default
		classToDataType.put(UUID.class, DataType.uuid());

		// override LocalTime to time as time columns are mapped to long by default
		classToDataType.put(LocalTime.class, DataType.time());

		return classToDataType;
	}

	/**
	 * @return the map between {@link Name} and {@link DataType}.
	 */
	private static Map<Name, DataType> nameToDataType() {

		Map<Name, DataType> nameToDataType = new HashMap<>(16);

		DataType.allPrimitiveTypes().forEach(dataType -> nameToDataType.put(dataType.getName(), dataType));

		nameToDataType.put(Name.VARCHAR, DataType.varchar());
		nameToDataType.put(Name.TEXT, DataType.text());

		return nameToDataType;
	}

	/**
	 * Returns a {@link Set} containing all Cassandra primitive types.
	 *
	 * @param codecRegistry the Cassandra codec registry.
	 * @return the set of Cassandra primitive types.
	 */
	private static Set<Class<?>> getCassandraPrimitiveTypes(CodecRegistry codecRegistry) {

		return DataType.allPrimitiveTypes().stream()
				.map(codecRegistry::codecFor)
				.map(TypeCodec::getJavaType)
				.map(TypeToken::getRawType)
				.collect(Collectors.toSet());
	}

	/**
	 * Returns the default {@link DataType} for a {@link Class}. This method resolves only simple types to a Cassandra
	 * {@link DataType}. Other types are resolved to {@literal null}.
	 *
	 * @param javaType must not be {@literal null}.
	 * @return the {@link DataType} for {@code javaClass} if resolvable, otherwise {@literal null}.
	 */
	@Nullable
	public static DataType getDataTypeFor(Class<?> javaType) {
		return javaType.isEnum() ? DataType.varchar() : classToDataType.get(javaType);
	}

	/**
	 * Returns the {@link DataType} for a {@link DataType.Name}.
	 *
	 * @param dataTypeName must not be {@literal null}.
	 * @return the {@link DataType} for {@link DataType.Name}.
	 */
	public static DataType getDataTypeFor(DataType.Name dataTypeName) {
		return nameToDataType.get(dataTypeName);
	}
}
