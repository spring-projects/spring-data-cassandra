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
package org.springframework.data.cassandra.convert;

import java.util.Optional;

import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.EntityConverter;
import org.springframework.data.util.TypeInformation;

/**
 * Central Cassandra specific converter interface from Object to Row.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public interface CassandraConverter
		extends EntityConverter<CassandraPersistentEntity<?>, CassandraPersistentProperty, Object, Object> {

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	@Override
	CassandraMappingContext getMappingContext();

	/**
	 * Returns the Id for an entity. It can return:
	 * <ul>
	 * <li>A singular value if for a simple {@link org.springframework.data.annotation.Id} or
	 * {@link org.springframework.data.cassandra.mapping.PrimaryKey} Id</li>
	 * <li>A {@link org.springframework.data.cassandra.repository.MapId} for composite
	 * {@link org.springframework.data.cassandra.mapping.PrimaryKeyColumn} Id's</li>
	 * <li>A the composite primary key for {@link org.springframework.data.cassandra.mapping.PrimaryKey} using a
	 * {@link org.springframework.data.cassandra.mapping.PrimaryKeyClass}</li>
	 * </ul>
	 *
	 * @param object must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return
	 */
	Object getId(Object object, CassandraPersistentEntity<?> entity);

	/**
	 * Converts and writes a {@code source} object into a {@code sink} using the given {@link CassandraPersistentEntity}.
	 *
	 * @param source the source, may be {@literal null}.
	 * @param sink must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 */
	void write(Object source, Object sink, CassandraPersistentEntity<?> entity);

	/**
	 * Converts the given object into one Cassandra will be able to store natively in a column.
	 *
	 * @param obj {@link Object} to convert, must not be {@literal null}.
	 * @param typeInformation {@link TypeInformation} used to describe the object type; must not be {@literal null}.
	 * @return the result of the conversion.
	 * @since 1.5
	 */
	<T> Optional<Object> convertToCassandraColumn(Optional<T> obj, TypeInformation<?> typeInformation);

	/**
	 * Returns the {@link CustomConversions} registered in the {@link CassandraConverter}.
	 *
	 * @return the {@link CustomConversions}.
	 */
	CustomConversions getCustomConversions();
}
