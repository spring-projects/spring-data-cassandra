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
package org.springframework.data.cassandra.repository.support;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * {@link CassandraEntityInformation} implementation using a {@link CassandraPersistentEntity} instance to lookup the
 * necessary information.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class MappingCassandraEntityInformation<T, ID> extends PersistentEntityInformation<T, ID>
		implements CassandraEntityInformation<T, ID> {

	private final CassandraPersistentEntity<T> entityMetadata;

	private final CassandraConverter converter;

	/**
	 * Create a new {@link MappingCassandraEntityInformation} for the given {@link CassandraPersistentEntity}.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public MappingCassandraEntityInformation(CassandraPersistentEntity<T> entity, CassandraConverter converter) {

		super(entity);

		this.entityMetadata = entity;
		this.converter = converter;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.EntityInformation#getId(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	@Nullable
	public ID getId(T entity) {

		Assert.notNull(entity, "Entity must not be null");

		CassandraPersistentProperty idProperty = this.entityMetadata.getIdProperty();

		return idProperty != null ? (ID) this.entityMetadata.getIdentifierAccessor(entity).getIdentifier()
				: (ID) converter.getId(entity, entityMetadata);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.EntityInformation#getIdType()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Class<ID> getIdType() {

		if (this.entityMetadata.getIdProperty() != null) {
			return (Class<ID>) this.entityMetadata.getRequiredIdProperty().getType();
		}

		return (Class<ID>) MapId.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraEntityInformation#getIdAttribute()
	 */
	@Override
	public String getIdAttribute() {
		return this.entityMetadata.getRequiredIdProperty().getName();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraEntityMetadata#getTableName()
	 */
	@Override
	public CqlIdentifier getTableName() {
		return this.entityMetadata.getTableName();
	}
}
