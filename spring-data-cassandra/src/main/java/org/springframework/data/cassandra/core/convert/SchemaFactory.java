/*
 * Copyright 2020-present the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.SpecificationBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.EmbeddedEntityOperations;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.domain.Vector;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Factory for Cassandra Schema objects such as user-defined types, tables and indexes.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Aleksei Zotov
 * @since 3.0
 * @see CreateUserTypeSpecification
 * @see CreateTableSpecification
 * @see CreateIndexSpecification
 * @see org.springframework.data.cassandra.core.mapping.CassandraMappingContext
 */
public class SchemaFactory {

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final ColumnTypeResolver typeResolver;

	private final EmbeddedEntityOperations embeddedEntityOperations;

	/**
	 * Creates a new {@link SchemaFactory} given {@link CassandraConverter}.
	 *
	 * @param converter must not be null.
	 */
	public SchemaFactory(CassandraConverter converter) {

		Assert.notNull(converter, "CassandraConverter must not be null");

		this.mappingContext = converter.getMappingContext();
		this.typeResolver = new DefaultColumnTypeResolver(mappingContext, ShallowUserTypeResolver.INSTANCE,
				converter::getCodecRegistry, converter::getCustomConversions);
		this.embeddedEntityOperations = new EmbeddedEntityOperations(this.mappingContext);
	}

	/**
	 * Creates a new {@link SchemaFactory} given {@link MappingContext}, {@link CustomConversions} and
	 * {@link CodecRegistry}.
	 *
	 * @param mappingContext must not be null.
	 * @param customConversions must not be null.
	 * @param codecRegistry must not be null.
	 */
	public SchemaFactory(
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext,
			CustomConversions customConversions, CodecRegistry codecRegistry) {

		Assert.notNull(mappingContext, "MappingContext must not be null");
		Assert.notNull(customConversions, "CustomConversions must not be null");
		Assert.notNull(codecRegistry, "CodecRegistry must not be null");

		this.mappingContext = mappingContext;
		this.typeResolver = new DefaultColumnTypeResolver(mappingContext, ShallowUserTypeResolver.INSTANCE,
				() -> codecRegistry, () -> customConversions);
		this.embeddedEntityOperations = new EmbeddedEntityOperations(this.mappingContext);
	}

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 *
	 * @param entityType must not be {@literal null}.
	 * @return the {@link CreateTableSpecification} derived from {@code entityType}.
	 */
	public CreateTableSpecification getCreateTableSpecificationFor(Class<?> entityType) {

		Assert.notNull(entityType, "Entity type must not be null");

		return getCreateTableSpecificationFor(mappingContext.getRequiredPersistentEntity(entityType));
	}

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the {@link CreateTableSpecification} derived from {@link CassandraPersistentEntity}.
	 */
	public CreateTableSpecification getCreateTableSpecificationFor(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return getCreateTableSpecificationFor(entity, entity.getTableName());
	}

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity using {@link CqlIdentifier table name}, including
	 * all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return
	 * @since 2.2
	 */
	@SuppressWarnings("NullAway")
	public CreateTableSpecification getCreateTableSpecificationFor(CassandraPersistentEntity<?> entity,
			CqlIdentifier tableName) {

		Assert.notNull(tableName, "Table name must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		CreateTableSpecification specification = SpecificationBuilder.createTable(entity.getKeyspace(), tableName);

		for (CassandraPersistentProperty property : entity) {

			if (property.isCompositePrimaryKey()) {

				CassandraPersistentEntity<?> primaryKeyEntity = mappingContext
						.getRequiredPersistentEntity(property.getRawType());

				for (CassandraPersistentProperty primaryKeyProperty : primaryKeyEntity) {

					DataType dataType = getDataType(primaryKeyProperty);

					if (primaryKeyProperty.isPartitionKeyColumn()) {
						specification.partitionKeyColumn(primaryKeyProperty.getRequiredColumnName(), dataType);
					} else { // cluster column
						specification.clusteredKeyColumn(primaryKeyProperty.getRequiredColumnName(), dataType,
								primaryKeyProperty.getPrimaryKeyOrdering());
					}
				}
			} else if (property.isEmbedded()) {

				CassandraPersistentEntity<?> embeddedEntity = embeddedEntityOperations.getEntity(property);

				for (CassandraPersistentProperty embeddedProperty : embeddedEntity) {

					DataType dataType = getDataType(embeddedProperty);
					specification.column(embeddedProperty.getRequiredColumnName(), dataType);
				}
			} else {
				DataType type = UserTypeUtil.potentiallyFreeze(getDataType(property));

				if (property.isIdProperty() || property.isPartitionKeyColumn()) {
					specification.partitionKeyColumn(property.getRequiredColumnName(), type);
				} else if (property.isClusterKeyColumn()) {
					specification.clusteredKeyColumn(property.getRequiredColumnName(), type, property.getPrimaryKeyOrdering());
				} else if (property.isStaticColumn()) {
					specification.staticColumn(property.getRequiredColumnName(), type);
				} else {
					specification.column(property.getRequiredColumnName(), type);
				}
			}
		}

		if (specification.getPartitionKeyColumns().isEmpty()) {
			throw new MappingException(String.format("No partition key columns found in entity [%s]", entity.getType()));
		}

		return specification;
	}

	private DataType getDataType(CassandraPersistentProperty property) {

		try {
			return typeResolver.resolve(property).getDataType();
		} catch (MappingException e) {

			if (isVector(property.getType())) {

				throw new MappingException(String.format(
						"Cannot resolve DataType for type [%s] for property [%s] in entity [%s]; Annotate the vector property with @VectorType(…)",
						property.getType(), property.getName(), property.getOwner().getName()), e);
			}

			throw new MappingException(String.format(
					"Cannot resolve DataType for type [%s] for property [%s] in entity [%s]; Consider registering a Converter or annotating the property with @CassandraType",
					property.getType(), property.getName(), property.getOwner().getName()), e);
		}
	}

	/**
	 * Returns {@link CreateIndexSpecification index specifications} derived from {@link CassandraPersistentEntity}.
	 *
	 * @param entityType must not be {@literal null}.
	 * @return the {@link CreateTableSpecification} derived from {@code entityType}.
	 */
	public List<CreateIndexSpecification> getCreateIndexSpecificationsFor(Class<?> entityType) {

		Assert.notNull(entityType, "Entity type must not be null");

		return getCreateIndexSpecificationsFor(mappingContext.getRequiredPersistentEntity(entityType));
	}

	/**
	 * Returns {@link CreateIndexSpecification index specifications} derived from {@link CassandraPersistentEntity}.
	 *
	 * @param entity must not be {@literal null}.
	 * @return
	 * @since 2.0
	 */
	public List<CreateIndexSpecification> getCreateIndexSpecificationsFor(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return getCreateIndexSpecificationsFor(entity, entity.getKeyspace(), entity.getTableName());
	}

	/**
	 * Returns {@link CreateIndexSpecification index specifications} derived from {@link CassandraPersistentEntity} using
	 * {@link CqlIdentifier table name}.
	 *
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return
	 * @since 2.0
	 */
	public List<CreateIndexSpecification> getCreateIndexSpecificationsFor(CassandraPersistentEntity<?> entity,
			CqlIdentifier tableName) {
		return getCreateIndexSpecificationsFor(entity, null, tableName);
	}

	/**
	 * Returns {@link CreateIndexSpecification index specifications} derived from {@link CassandraPersistentEntity} using
	 * {@link CqlIdentifier table name}.
	 *
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return
	 * @since 2.0
	 */
	public List<CreateIndexSpecification> getCreateIndexSpecificationsFor(CassandraPersistentEntity<?> entity,
			@Nullable CqlIdentifier keyspace, CqlIdentifier tableName) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		List<CreateIndexSpecification> indexes = new ArrayList<>();

		for (CassandraPersistentProperty property : entity) {

			if (property.isCompositePrimaryKey()) {
				CassandraPersistentEntity<?> pkEntity = mappingContext.getRequiredPersistentEntity(property);
				indexes.addAll(getCreateIndexSpecificationsFor(pkEntity, pkEntity.getKeyspace(), pkEntity.getTableName()));
			}

			if (property.isEmbedded()) {

				if (property.isAnnotationPresent(Indexed.class)) {
					Indexed indexed = property.getRequiredAnnotation(Indexed.class);
					for (CassandraPersistentProperty embeddedProperty : embeddedEntityOperations.getEntity(property)) {
						indexes.add(IndexSpecificationFactory.createIndexSpecification(keyspace, indexed, embeddedProperty));
					}
				} else {
					indexes.addAll(getCreateIndexSpecificationsFor(embeddedEntityOperations.getEntity(property)));
				}
			} else {
				indexes.addAll(IndexSpecificationFactory.createIndexSpecifications(keyspace, property));
			}
		}

		indexes.forEach(it -> it.tableName(entity.getTableName()));

		return indexes;
	}

	/**
	 * Returns a {@link CreateUserTypeSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public CreateUserTypeSpecification getCreateUserTypeSpecificationFor(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		CreateUserTypeSpecification specification = SpecificationBuilder.createType(entity.getKeyspace(),
				entity.getTableName());

		for (CassandraPersistentProperty property : entity) {

			if (property.isEmbedded()) {

				CassandraPersistentEntity<?> embeddedEntity = embeddedEntityOperations.getEntity(property);
				for (CassandraPersistentProperty embeddedProperty : embeddedEntity) {

					DataType dataType = getDataType(embeddedProperty);
					specification.field(embeddedProperty.getRequiredColumnName(), dataType);
				}
			} else {
				// Use frozen literal to not resolve types from Cassandra; At this stage, they might be not created yet.
				specification.field(property.getRequiredColumnName(), UserTypeUtil.potentiallyFreeze(getDataType(property)));
			}
		}

		if (specification.getFields().isEmpty()) {
			throw new MappingException(String.format("No fields in user type [%s]", entity.getType()));
		}

		return specification;
	}

	private static boolean isVector(Class<?> type) {
		return type.equals(CqlVector.class) || Vector.class.isAssignableFrom(type);
	}

	enum ShallowUserTypeResolver implements UserTypeResolver {

		INSTANCE;

		@Override
		public UserDefinedType resolveType(CqlIdentifier typeName) {
			return new ShallowUserDefinedType(null, typeName, false);
		}

		@Override
		public UserDefinedType resolveType(CqlIdentifier keyspace, CqlIdentifier typeName) {
			return new ShallowUserDefinedType(keyspace, typeName, false);
		}

	}

	static class ShallowUserDefinedType implements com.datastax.oss.driver.api.core.type.UserDefinedType {

		private final @Nullable CqlIdentifier keyspace;
		private final CqlIdentifier name;
		private final boolean frozen;

		public ShallowUserDefinedType(String name, boolean frozen) {
			this(null, CqlIdentifier.fromInternal(name), frozen);
		}

		public ShallowUserDefinedType(@Nullable CqlIdentifier keyspace, CqlIdentifier name, boolean frozen) {
			this.keyspace = keyspace;
			this.name = name;
			this.frozen = frozen;
		}

		@Override
		public @Nullable CqlIdentifier getKeyspace() {
			return keyspace;
		}

		@Override
		public CqlIdentifier getName() {
			return name;
		}

		@Override
		public boolean isFrozen() {
			return frozen;
		}

		@Override
		public List<CqlIdentifier> getFieldNames() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public int firstIndexOf(CqlIdentifier id) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public int firstIndexOf(String name) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public List<DataType> getFieldTypes() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public com.datastax.oss.driver.api.core.type.UserDefinedType copy(boolean newFrozen) {
			return new ShallowUserDefinedType(this.keyspace, this.name, newFrozen);
		}

		@Override
		public UdtValue newValue() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public UdtValue newValue(@NonNull Object... fields) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public AttachmentPoint getAttachmentPoint() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public boolean isDetached() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public void attach(@NonNull AttachmentPoint attachmentPoint) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o)
				return true;
			if (!(o instanceof UserDefinedType that))
				return false;
			return isFrozen() == that.isFrozen() && Objects.equals(getName(), that.getName());
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, frozen);
		}

		@Override
		public String toString() {
			return "UDT(" + name.asCql(true) + ")";
		}
	}

}
