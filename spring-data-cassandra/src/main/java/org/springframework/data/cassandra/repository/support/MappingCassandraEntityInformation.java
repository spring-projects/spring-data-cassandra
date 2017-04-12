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
package org.springframework.data.cassandra.repository.support;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.repository.core.support.AbstractEntityInformation;
import org.springframework.util.Assert;

/**
 * {@link CassandraEntityInformation} implementation using a {@link CassandraPersistentEntity} instance to lookup the
 * necessary information. Can be configured with a custom collection to be returned which will trump the one returned by
 * the {@link CassandraPersistentEntity} if given.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class MappingCassandraEntityInformation<T, ID extends Serializable> extends AbstractEntityInformation<T, ID>
		implements CassandraEntityInformation<T, ID> {

	private final CassandraPersistentEntity<T> entityMetadata;
	private final CassandraConverter converter;
	private final boolean isPrimaryKeyEntity;

	/**
	 * Creates a new {@link MappingCassandraEntityInformation} for the given {@link CassandraPersistentEntity}.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public MappingCassandraEntityInformation(CassandraPersistentEntity<T> entity, CassandraConverter converter) {

		super(entity.getType());

		this.entityMetadata = entity;
		this.converter = converter;
		this.isPrimaryKeyEntity = hasNonIdProperties(entity);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ID getId(T entity) {

		Assert.notNull(entity, "Entity must not be null");

		CassandraPersistentProperty idProperty = entityMetadata.getIdProperty();

		if (idProperty != null) {
			return (ID) entityMetadata.getIdentifierAccessor(entity).getIdentifier();
		}

		return (ID) converter.getId(entity, entityMetadata);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<ID> getIdType() {
		return (Class<ID>) (entityMetadata.getIdProperty() == null ? MapId.class
				: entityMetadata.getIdProperty().getType());
	}

	@Override
	public CqlIdentifier getTableName() {
		return entityMetadata.getTableName();
	}

	@Override
	public boolean isPrimaryKeyEntity() {
		return isPrimaryKeyEntity;
	}

	private static boolean hasNonIdProperties(CassandraPersistentEntity<?> entity) {

		final AtomicReference<Boolean> hasPrimaryKeyOnlyProperties = new AtomicReference<Boolean>(true);

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty property) {

				if (property.isCompositePrimaryKey() || property.isPrimaryKeyColumn() || property.isIdProperty()) {
					return;
				}

				hasPrimaryKeyOnlyProperties.set(false);
			}
		});

		return hasPrimaryKeyOnlyProperties.get();
	}
}
