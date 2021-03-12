/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

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

	private static final List<DataType> primitives = Arrays.asList(DataTypes.ASCII, DataTypes.BIGINT, DataTypes.BLOB,
			DataTypes.BOOLEAN, DataTypes.COUNTER, DataTypes.DECIMAL, DataTypes.DOUBLE, DataTypes.FLOAT, DataTypes.INT,
			DataTypes.TIMESTAMP, DataTypes.UUID, DataTypes.VARINT, DataTypes.TIMEUUID, DataTypes.INET, DataTypes.DATE,
			DataTypes.TEXT, DataTypes.TIME, DataTypes.SMALLINT, DataTypes.TINYINT, DataTypes.DURATION);

	private static final Map<Class<?>, DataType> classToDataType;

	private static final Map<CassandraType.Name, DataType> nameToDataType;

	static {

		CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

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
		simpleTypes.add(UdtValue.class);
		simpleTypes.add(CqlDuration.class);

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

		primitives.forEach(dataType -> {
			Class<?> javaType = codecRegistry.codecFor(dataType).getJavaType().getRawType();

			classToDataType.put(javaType, dataType);

			Optional.ofNullable(primitiveWrappers.get(javaType))
					.ifPresent(primitiveType -> classToDataType.put(primitiveType, dataType));
		});

		// map Long to bigint as counter columns (last type aver multiple overrides) are a special use case
		// so map it to a more common type by default
		classToDataType.put(Long.class, DataTypes.BIGINT);
		classToDataType.put(long.class, DataTypes.BIGINT);

		// override UUID to timeuuid as regular uuid as the favored default
		classToDataType.put(UUID.class, DataTypes.UUID);

		// Add type migration support for TIMESTAMP via java.util.Date to preserve driver v3 type mapping
		classToDataType.put(Date.class, DataTypes.TIMESTAMP);

		return classToDataType;
	}

	/**
	 * @return the map between {@link CassandraType.Name} and {@link DataType}.
	 */
	private static Map<CassandraType.Name, DataType> nameToDataType() {

		Map<CassandraType.Name, DataType> nameToDataType = new HashMap<>(16);

		nameToDataType.put(CassandraType.Name.ASCII, DataTypes.ASCII);
		nameToDataType.put(CassandraType.Name.BIGINT, DataTypes.BIGINT);
		nameToDataType.put(CassandraType.Name.BLOB, DataTypes.BLOB);
		nameToDataType.put(CassandraType.Name.BOOLEAN, DataTypes.BOOLEAN);
		nameToDataType.put(CassandraType.Name.COUNTER, DataTypes.COUNTER);
		nameToDataType.put(CassandraType.Name.DECIMAL, DataTypes.DECIMAL);
		nameToDataType.put(CassandraType.Name.DOUBLE, DataTypes.DOUBLE);
		nameToDataType.put(CassandraType.Name.FLOAT, DataTypes.FLOAT);
		nameToDataType.put(CassandraType.Name.INT, DataTypes.INT);
		nameToDataType.put(CassandraType.Name.TIMESTAMP, DataTypes.TIMESTAMP);
		nameToDataType.put(CassandraType.Name.UUID, DataTypes.UUID);
		nameToDataType.put(CassandraType.Name.VARCHAR, DataTypes.TEXT);
		nameToDataType.put(CassandraType.Name.TEXT, DataTypes.TEXT);
		nameToDataType.put(CassandraType.Name.TIMEUUID, DataTypes.TIMEUUID);
		nameToDataType.put(CassandraType.Name.INET, DataTypes.INET);
		nameToDataType.put(CassandraType.Name.DATE, DataTypes.DATE);
		nameToDataType.put(CassandraType.Name.SMALLINT, DataTypes.SMALLINT);
		nameToDataType.put(CassandraType.Name.TINYINT, DataTypes.TINYINT);
		nameToDataType.put(CassandraType.Name.VARINT, DataTypes.VARINT);
		nameToDataType.put(CassandraType.Name.TIME, DataTypes.TIME);
		nameToDataType.put(CassandraType.Name.DURATION, DataTypes.DURATION);

		return nameToDataType;
	}

	/**
	 * Returns a {@link Set} containing all Cassandra primitive types.
	 *
	 * @param codecRegistry the Cassandra codec registry.
	 * @return the set of Cassandra primitive types.
	 */
	private static Set<Class<?>> getCassandraPrimitiveTypes(CodecRegistry codecRegistry) {

		return primitives.stream().map(codecRegistry::codecFor).map(TypeCodec::getJavaType).map(GenericType::getRawType)
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
		return javaType.isEnum() ? DataTypes.TEXT : classToDataType.get(javaType);
	}

	/**
	 * Returns the required default {@link DataType} for a {@link Class}. This method resolves only simple types to a
	 * Cassandra {@link DataType}. Throws {@link IllegalStateException} if the {@link Class} cannot be resolved to a
	 * {@link DataType}.
	 *
	 * @param javaType must not be {@literal null}.
	 * @return the {@link DataType} for {@code javaClass} if resolvable, otherwise {@literal null}.
	 * @throws IllegalStateException if the {@link Class} cannot be resolved to a {@link DataType}.
	 * @since 3.1.6
	 * @see #getDataTypeFor(Class)
	 */
	public static DataType getRequiredDataTypeFor(Class<?> javaType) {

		DataType dataType = getDataTypeFor(javaType);

		if (dataType == null) {
			throw new IllegalStateException(String.format("Required DataType cannot be resolved for %s", javaType.getName()));
		}

		return dataType;
	}

	/**
	 * Returns the {@link DataType} for a {@link CassandraType.Name}.
	 *
	 * @param dataTypeName must not be {@literal null}.
	 * @return the {@link DataType} for {@link CassandraType.Name}.
	 */
	@Nullable
	public static DataType getDataTypeFor(CassandraType.Name dataTypeName) {
		return nameToDataType.get(dataTypeName);
	}

	/**
	 * Returns the required {@link DataType} for a {@link CassandraType.Name}. Throws {@link IllegalStateException} if the
	 * {@link CassandraType.Name} cannot be resolved to a {@link DataType}.
	 *
	 * @param dataTypeName must not be {@literal null}.
	 * @return the {@link DataType} for {@link CassandraType.Name}.
	 * @throws IllegalStateException if the {@link CassandraType.Name} cannot be resolved to a {@link DataType}.
	 * @since 3.1.6
	 * @see #getDataTypeFor(CassandraType.Name)
	 */
	public static DataType getRequiredDataTypeFor(CassandraType.Name dataTypeName) {

		DataType dataType = getDataTypeFor(dataTypeName);

		if (dataType == null) {
			throw new IllegalStateException(String.format("Required DataType cannot be resolved for %s", dataTypeName));
		}

		return dataType;
	}

}
