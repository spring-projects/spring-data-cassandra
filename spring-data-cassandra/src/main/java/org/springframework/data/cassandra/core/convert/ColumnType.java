/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.convert;

import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Interface to access column type information. The {@link CassandraColumnType} subtype exposes Cassandra-specific
 * {@link DataType} information.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public interface ColumnType {

	/**
	 * Creates a {@link ColumnType} for a {@link Class}.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	static ColumnType create(Class<?> type) {
		return create(ClassTypeInformation.from(type));
	}

	/**
	 * Creates a {@link ColumnType} for a {@link TypeInformation}.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	static ColumnType create(TypeInformation<?> type) {
		return new DefaultColumnType(type);
	}

	/**
	 * Creates a {@link ColumnType} for a {@link Class} and {@link DataType}.
	 *
	 * @param type must not be {@literal null}.
	 * @param dataType must not be {@literal null}.
	 * @return
	 */
	static CassandraColumnType create(Class<?> type, DataType dataType) {
		return new DefaultCassandraColumnType(ClassTypeInformation.from(type), dataType);
	}

	/**
	 * Creates a List {@link ColumnType} given its {@link ColumnType component type}.
	 *
	 * @param componentType must not be {@literal null}.
	 * @return
	 */
	static ColumnType listOf(ColumnType componentType) {

		if (componentType instanceof CassandraColumnType) {
			return listOf((CassandraColumnType) componentType);
		}

		return new DefaultColumnType(ClassTypeInformation.LIST, componentType);
	}

	/**
	 * Creates a List {@link ColumnType} given its {@link CassandraColumnType component type}.
	 *
	 * @param componentType must not be {@literal null}.
	 * @return
	 */
	static CassandraColumnType listOf(CassandraColumnType componentType) {
		return listOf(componentType, false);
	}

	/**
	 * Creates a List {@link ColumnType} given its {@link CassandraColumnType component type}.
	 *
	 * @param componentType must not be {@literal null}.
	 * @param frozen
	 * @return
	 */
	static CassandraColumnType listOf(CassandraColumnType componentType, boolean frozen) {
		return new DefaultCassandraColumnType(ClassTypeInformation.LIST,
				() -> DataTypes.listOf(componentType.getDataType(), frozen), componentType);
	}

	/**
	 * Creates a Set {@link ColumnType} given its {@link ColumnType component type}.
	 *
	 * @param componentType must not be {@literal null}.
	 * @return
	 */
	static ColumnType setOf(ColumnType componentType) {

		if (componentType instanceof CassandraColumnType) {
			return setOf((CassandraColumnType) componentType);
		}

		return new DefaultColumnType(ClassTypeInformation.SET, componentType);
	}

	/**
	 * Creates a Set {@link ColumnType} given its {@link CassandraColumnType component type}.
	 *
	 * @param componentType must not be {@literal null}.
	 * @return
	 */
	static CassandraColumnType setOf(CassandraColumnType componentType) {
		return setOf(componentType, false);
	}

	/**
	 * Creates a Set {@link ColumnType} given its {@link CassandraColumnType component type}.
	 *
	 * @param componentType must not be {@literal null}.
	 * @param frozen
	 * @return
	 */
	static CassandraColumnType setOf(CassandraColumnType componentType, boolean frozen) {
		return new DefaultCassandraColumnType(ClassTypeInformation.SET,
				() -> DataTypes.setOf(componentType.getDataType(), frozen), componentType);
	}

	/**
	 * Creates a Map {@link ColumnType} given its {@link ColumnType key and value types}.
	 *
	 * @param keyType must not be {@literal null}.
	 * @param valueType must not be {@literal null}.
	 * @return
	 */
	static ColumnType mapOf(ColumnType keyType, ColumnType valueType) {
		return new DefaultColumnType(ClassTypeInformation.MAP, keyType, valueType);
	}

	/**
	 * Creates a Map {@link CassandraColumnType} given its {@link CassandraColumnType key and value types}.
	 *
	 * @param keyType must not be {@literal null}.
	 * @param valueType must not be {@literal null}.
	 * @return
	 */
	static CassandraColumnType mapOf(CassandraColumnType keyType, CassandraColumnType valueType) {
		return new DefaultCassandraColumnType(ClassTypeInformation.MAP,
				() -> DataTypes.mapOf(keyType.getDataType(), valueType.getDataType()), keyType, valueType);
	}

	/**
	 * Creates a UDT {@link CassandraColumnType} given its {@link UserDefinedType Cassandra type}.
	 *
	 * @param dataType must not be {@literal null}.
	 * @return
	 */
	static CassandraColumnType udtOf(UserDefinedType dataType) {
		return new DefaultCassandraColumnType(UdtValue.class, dataType);
	}

	/**
	 * Creates a Tuple {@link CassandraColumnType} given its {@link TupleType Cassandra type}.
	 *
	 * @param dataType must not be {@literal null}.
	 * @return
	 */
	static CassandraColumnType tupleOf(TupleType dataType) {
		return new DefaultCassandraColumnType(TupleValue.class, dataType);
	}

	/**
	 * Returns the Java type of the column.
	 *
	 * @return
	 */
	Class<?> getType();

	/**
	 * Returns whether the type can be considered a collection, which means it's a container of elements, e.g. a
	 * {@link java.util.Collection} and {@link java.lang.reflect.Array} or anything implementing {@link Iterable}. If this
	 * returns {@literal true} you can expect {@link #getComponentType()} to return a non-{@literal null} value.
	 *
	 * @return
	 */
	boolean isCollectionLike();

	/**
	 * Returns whether the property is a {@link java.util.List}. If this returns {@literal true} you can expect
	 * {@link #getComponentType()} to return something not {@literal null}.
	 *
	 * @return
	 */
	boolean isList();

	/**
	 * Returns whether the property is a {@link java.util.Set}. If this returns {@literal true} you can expect
	 * {@link #getComponentType()} to return something not {@literal null}.
	 *
	 * @return
	 */
	boolean isSet();

	/**
	 * Returns whether the property is a {@link java.util.Map}. If this returns {@literal true} you can expect
	 * {@link #getComponentType()} as well as {@link #getMapValueType()} to return something not {@literal null}.
	 *
	 * @return
	 */
	boolean isMap();

	/**
	 * Returns the component type for {@link java.util.Collection}s or the key type for {@link java.util.Map}s.
	 *
	 * @return
	 */
	@Nullable
	ColumnType getComponentType();

	/**
	 * Returns the component type for {@link java.util.Collection}s, the key type for {@link java.util.Map}s or the single
	 * generic type if available. Throws {@link IllegalStateException} if the component value type cannot be resolved.
	 *
	 * @return
	 * @throws IllegalStateException if the component type cannot be resolved, e.g. if a raw type is used or the type is
	 *           not generic in the first place.
	 */
	default ColumnType getRequiredComponentType() {

		ColumnType columnType = getComponentType();

		if (columnType == null) {
			throw new IllegalStateException("Type has no component type");
		}

		return columnType;
	}

	/**
	 * Returns the map value type in case the underlying type is a {@link java.util.Map}.
	 *
	 * @return
	 */
	@Nullable
	ColumnType getMapValueType();

	/**
	 * Returns the map value type in case the underlying type is a {@link java.util.Map}. or throw
	 * {@link IllegalStateException} if the map value type cannot be resolved.
	 *
	 * @return
	 * @throws IllegalStateException if the map value type cannot be resolved, usually due to the current
	 *           {@link java.util.Map} type being a raw one.
	 */
	default ColumnType getRequiredMapValueType() {

		ColumnType columnType = getMapValueType();

		if (columnType == null) {
			throw new IllegalStateException("Type has no map value type");
		}

		return columnType;
	}

}
