/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.cassandra.core;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.update.Update;

/**
 * Common data access operations performed on an entity using a {@link MappingContext} containing mapping metadata.
 *
 * @author Mark Paluch
 * @author John Blum
 * @see CassandraTemplate
 * @see AsyncCassandraTemplate
 * @see ReactiveCassandraTemplate
 * @since 2.2
 */
class EntityOperations {

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	public EntityOperations(
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {
		this.mappingContext = mappingContext;
	}

	/**
	 * Creates a new {@link Entity} for the given bean.
	 *
	 * @param entity must not be {@literal null}.
	 * @return
	 */
	public <T> Entity<T> forEntity(T entity) {

		Assert.notNull(entity, "Bean must not be null");

		return MappedEntity.of(entity, getMappingContext());
	}

	/**
	 * Creates a new {@link AdaptibleEntity} for the given bean and {@link ConversionService}.
	 *
	 * @param entity must not be {@literal null}.
	 * @param conversionService must not be {@literal null}.
	 * @return
	 */
	public <T> AdaptibleEntity<T> forEntity(T entity, ConversionService conversionService) {

		Assert.notNull(entity, "Bean must not be null");
		Assert.notNull(conversionService, "ConversionService must not be null");

		return AdaptibleMappedEntity.of(entity, getMappingContext(), conversionService);
	}

	/**
	 * Returns the {@link MappingContext} used by this entity data access operations class to access mapping meta-data
	 * used to store (map) object to Cassandra tables.
	 *
	 * @return the {@link MappingContext} used by this entity data access operations class.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraMappingContext
	 */
	CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityClass) {
		return getMappingContext().getRequiredPersistentEntity(ClassUtils.getUserClass(entityClass));
	}

	/**
	 * Returns the table name to which the entity shall be persisted.
	 *
	 * @param entityClass entity class, must not be {@literal null}.
	 * @return the table name to which the entity shall be persisted.
	 */
	CqlIdentifier getTableName(Class<?> entityClass) {
		return getRequiredPersistentEntity(entityClass).getTableName();
	}


	protected MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	/**
	 * A representation of information about an entity.
	 */
	interface Entity<T> {

		/**
		 * Returns whether the entity is versioned, i.e. if it contains a version property.
		 *
		 * @return
		 */
		default boolean isVersionedEntity() {
			return false;
		}

		/**
		 * Returns the value of the version if the entity has a version property, {@literal null} otherwise.
		 *
		 * @return
		 */
		@Nullable
		Object getVersion();

		/**
		 * Returns the underlying bean.
		 *
		 * @return
		 */
		T getBean();

		/**
		 * Returns whether the entity is considered to be new.
		 *
		 * @return
		 */
		boolean isNew();
	}

	/**
	 * Information and commands on an entity.
	 */
	interface AdaptibleEntity<T> extends Entity<T> {

		/**
		 * Appends a {@code IF} condition to an {@link Update} statement for optimistic locking to perform the update only
		 * if the version number matches. This method accepts {@code currentVersionNumber} as the {@link Update} typically
		 * requires to increment the version number upon assembly time.
		 *
		 * @param update the {@link Update} statement to append the condition to.
		 * @param currentVersionNumber previous version number.
		 * @return the altered {@link Update} containing the {@code IF} condition for optimistic locking.
		 */
		StatementBuilder<Update> appendVersionCondition(StatementBuilder<Update> update, Number currentVersionNumber);

		/**
		 * Appends a {@code IF} condition to an {@link Delete} statement for optimistic locking to perform the delete only
		 * if the version number matches. The {@link #getVersion() version number} is derived from the actual state as
		 * delete statements typically do not increment the version prior to statement creation.
		 *
		 * @param delete the {@link Delete} statement to append the condition to.
		 * @return the altered {@link Delete} containing the {@code IF} condition for optimistic locking.
		 * @see #getVersion()
		 */
		StatementBuilder<Delete> appendVersionCondition(StatementBuilder<Delete> delete);

		/**
		 * Initializes the version property of the of the current entity if available.
		 *
		 * @return the entity with the version property updated if available.
		 */
		T initializeVersionProperty();

		/**
		 * Increments the value of the version property if available.
		 *
		 * @return the entity with the version property incremented if available.
		 */
		T incrementVersion();

		/**
		 * Returns the current version value if the entity has a version property.
		 *
		 * @return the current version or {@literal null} in case it's uninitialized or the entity doesn't expose a version
		 *         property.
		 */
		@Nullable
		Number getVersion();

		/**
		 * Returns the {@link CassandraPersistentEntity}.
		 *
		 * @return the {@link CassandraPersistentEntity}.
		 */
		CassandraPersistentEntity<?> getPersistentEntity();

	}

	private static class MappedEntity<T> implements Entity<T> {

		private final CassandraPersistentEntity<?> entity;
		private final PersistentPropertyAccessor<T> propertyAccessor;

		protected MappedEntity(CassandraPersistentEntity<?> entity, PersistentPropertyAccessor<T> propertyAccessor) {
			this.entity = entity;
			this.propertyAccessor = propertyAccessor;
		}

		private static <T> MappedEntity<T> of(T bean,
				MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> context) {

			CassandraPersistentEntity<?> entity = context.getRequiredPersistentEntity(bean.getClass());
			PersistentPropertyAccessor<T> propertyAccessor = entity.getPropertyAccessor(bean);

			return new MappedEntity<>(entity, propertyAccessor);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.Entity#getBean()
		 */
		@Override
		public T getBean() {
			return this.propertyAccessor.getBean();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.Entity#isNew()
		 */
		@Override
		public boolean isNew() {
			return this.entity.isNew(getBean());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.Entity#isVersionedEntity()
		 */
		@Override
		public boolean isVersionedEntity() {
			return this.entity.hasVersionProperty();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.Entity#getVersion()
		 */
		@Override
		@Nullable
		public Object getVersion() {
			return this.propertyAccessor.getProperty(this.entity.getRequiredVersionProperty());
		}
	}

	private static class AdaptibleMappedEntity<T> extends MappedEntity<T> implements AdaptibleEntity<T> {

		private final CassandraPersistentEntity<?> entity;
		private final ConvertingPropertyAccessor<T> propertyAccessor;

		private static <T> AdaptibleEntity<T> of(T bean,
				MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext,
				ConversionService conversionService) {

			CassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(bean.getClass());

			PersistentPropertyAccessor<T> propertyAccessor = entity.getPropertyAccessor(bean);

			return new AdaptibleMappedEntity<>(entity, new ConvertingPropertyAccessor<>(propertyAccessor, conversionService));
		}

		private AdaptibleMappedEntity(CassandraPersistentEntity<?> entity, ConvertingPropertyAccessor<T> propertyAccessor) {

			super(entity, propertyAccessor);

			this.entity = entity;
			this.propertyAccessor = propertyAccessor;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.AdaptibleEntity#appendVersionCondition(com.datastax.oss.driver.api.querybuilder.update.Update, java.lang.Number)
		 */
		@Override
		public StatementBuilder<Update> appendVersionCondition(StatementBuilder<Update> update,
				Number currentVersionNumber) {

			return update.bind((statement, factory) -> {
				return statement.if_(Condition.column(getVersionColumnName()).isEqualTo(factory.create(currentVersionNumber)));
			});
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.AdaptibleEntity#appendVersionCondition(com.datastax.oss.driver.api.querybuilder.delete.Delete)
		 */
		@Override
		public StatementBuilder<Delete> appendVersionCondition(StatementBuilder<Delete> delete) {

			return delete.bind((statement, factory) -> {
				return statement.if_(Condition.column(getVersionColumnName()).isEqualTo(factory.create(getVersion())));
			});
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.AdaptibleEntity#initializeVersionProperty()
		 */
		@Override
		public T initializeVersionProperty() {

			if (this.entity.hasVersionProperty()) {

				CassandraPersistentProperty versionProperty = this.entity.getRequiredVersionProperty();

				this.propertyAccessor.setProperty(versionProperty, versionProperty.getType().isPrimitive() ? 1 : 0);
			}

			return this.propertyAccessor.getBean();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.AdaptibleEntity#incrementVersion()
		 */
		@Override
		public T incrementVersion() {

			CassandraPersistentProperty versionProperty = this.entity.getRequiredVersionProperty();

			Number version = getVersion();
			Number nextVersion = version == null ? 0 : version.longValue() + 1;

			this.propertyAccessor.setProperty(versionProperty, nextVersion);

			return this.propertyAccessor.getBean();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.MappedEntity#getVersion()
		 */
		@Override
		@Nullable
		public Number getVersion() {

			CassandraPersistentProperty versionProperty = this.entity.getRequiredVersionProperty();

			return this.propertyAccessor.getProperty(versionProperty, Number.class);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.EntityOperations.AdaptibleEntity#getPersistentEntity()
		 */
		@Override
		public CassandraPersistentEntity<?> getPersistentEntity() {
			return this.entity;
		}

		private CqlIdentifier getVersionColumnName() {
			return this.entity.getRequiredVersionProperty().getColumnName();
		}
	}
}
