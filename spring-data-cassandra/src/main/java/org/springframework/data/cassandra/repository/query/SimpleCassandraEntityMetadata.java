/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Implementation of {@link CassandraEntityMetadata} based on the type and {@link CassandraPersistentEntity}.
 *
 * @author Mark Paluch
 * @since 1.5
 */
class SimpleCassandraEntityMetadata<T> implements CassandraEntityMetadata<T> {

	private final CassandraPersistentEntity<?> entity;

	private final Class<T> type;

	/**
	 * Create a new {@link SimpleCassandraEntityMetadata} using the given type and {@link CassandraPersistentEntity} to
	 * use for table lookups.
	 *
	 * @param type must not be {@literal null}.
	 * @param entity must not be {@literal null} or empty.
	 */
	SimpleCassandraEntityMetadata(Class<T> type, CassandraPersistentEntity<?> entity) {

		Assert.notNull(type, "Type must not be null");
		Assert.notNull(entity, "Collection entity must not be null or empty");

		this.type = type;
		this.entity = entity;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraEntityMetadata#getTableName()
	 */
	@Override
	public CqlIdentifier getTableName() {
		return entity.getTableName();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.EntityMetadata#getJavaType()
	 */
	@Override
	public Class<T> getJavaType() {
		return type;
	}
}
