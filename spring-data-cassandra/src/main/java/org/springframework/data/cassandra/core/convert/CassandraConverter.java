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
package org.springframework.data.cassandra.core.convert;

import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.EntityConverter;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Central Cassandra specific converter interface from Object to Row.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public interface CassandraConverter
		extends EntityConverter<CassandraPersistentEntity<?>, CassandraPersistentProperty, Object, Object> {

	/**
	 * Returns the {@link CustomConversions} registered in the {@link CassandraConverter}.
	 *
	 * @return the {@link CustomConversions}.
	 */
	CustomConversions getCustomConversions();

	/**
	 * Returns the {@link CodecRegistry} registered in the {@link CassandraConverter}.
	 *
	 * @return the {@link CodecRegistry}.
	 * @since 3.0
	 */
	CodecRegistry getCodecRegistry();

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	@Override
	CassandraMappingContext getMappingContext();

	/**
	 * Returns the {@link ColumnTypeResolver} to resolve {@link ColumnType} for properties, {@link TypeInformation}, and
	 * {@code values}.
	 *
	 * @return the {@link ColumnTypeResolver}
	 * @since 3.0
	 */
	ColumnTypeResolver getColumnTypeResolver();

	/**
	 * Returns the Id for an entity. It can return:
	 * <ul>
	 * <li>A singular value if for a simple {@link org.springframework.data.annotation.Id} or
	 * {@link org.springframework.data.cassandra.core.mapping.PrimaryKey} Id</li>
	 * <li>A {@link MapId} for composite {@link org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn}
	 * Id's</li>
	 * <li>A the composite primary key for {@link org.springframework.data.cassandra.core.mapping.PrimaryKey} using a
	 * {@link org.springframework.data.cassandra.core.mapping.PrimaryKeyClass}</li>
	 * </ul>
	 *
	 * @param object must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the id value or {@literal null}, if the id is not set.
	 */
	@Nullable
	Object getId(Object object, CassandraPersistentEntity<?> entity);

	/**
	 * Converts the given object into a value Cassandra will be able to store natively in a column.
	 *
	 * @param value {@link Object} to convert; must not be {@literal null}.
	 * @return the result of the conversion.
	 * @since 2.0
	 */
	Object convertToColumnType(Object value);

	/**
	 * Converts the given object into a value Cassandra will be able to store natively in a column.
	 *
	 * @param value {@link Object} to convert; must not be {@literal null}.
	 * @param typeInformation {@link TypeInformation} used to describe the object type; must not be {@literal null}.
	 * @return the result of the conversion.
	 * @since 1.5
	 */
	default Object convertToColumnType(Object value, TypeInformation<?> typeInformation) {

		Assert.notNull(value, "Value must not be null");
		Assert.notNull(typeInformation, "TypeInformation must not be null");

		return convertToColumnType(value, getColumnTypeResolver().resolve(typeInformation));
	}

	/**
	 * Converts the given object into a value Cassandra will be able to store natively in a column.
	 *
	 * @param value {@link Object} to convert; must not be {@literal null}.
	 * @param typeDescriptor {@link ColumnType} used to describe the object type; must not be {@literal null}.
	 * @return the result of the conversion.
	 * @since 3.0
	 */
	Object convertToColumnType(Object value, ColumnType typeDescriptor);

	/**
	 * Converts and writes a {@code source} object into a {@code sink} using the given {@link CassandraPersistentEntity}.
	 *
	 * @param source the source, must not be {@literal null}.
	 * @param sink must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 */
	void write(Object source, Object sink, CassandraPersistentEntity<?> entity);
}
