/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import static org.springframework.data.cassandra.core.cql.CqlIdentifier.*;
import static org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification.*;
import static org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateUserTypeSpecification;
import org.springframework.data.cassandra.core.mapping.UserTypeUtil.FrozenLiteralDataType;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Optionals;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.DataType;

/**
 * Default implementation of a {@link MappingContext} for Cassandra using {@link CassandraPersistentEntity} and
 * {@link CassandraPersistentProperty} as primary abstractions.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author John Blum
 * @author Jens Schauder
 */
public class CassandraMappingContext
		extends AbstractMappingContext<BasicCassandraPersistentEntity<?>, CassandraPersistentProperty>
		implements ApplicationContextAware, BeanClassLoaderAware {

	private CassandraPersistentEntityMetadataVerifier verifier = new CompositeCassandraPersistentEntityMetadataVerifier();

	private CustomConversions customConversions = new CustomConversions(
			StoreConversions.of(CassandraSimpleTypeHolder.HOLDER), Collections.emptyList());

	private Mapping mapping = new Mapping();

	private @Nullable UserTypeResolver userTypeResolver;

	private @Nullable ApplicationContext context;

	private @Nullable ClassLoader beanClassLoader;

	// useful caches
	private final Map<CqlIdentifier, Set<CassandraPersistentEntity<?>>> entitySetsByTableName = new HashMap<>();
	private final Set<BasicCassandraPersistentEntity<?>> userDefinedTypes = new HashSet<>();
	private final Set<BasicCassandraPersistentEntity<?>> tableEntities = new HashSet<>();

	/**
	 * Create a new {@link CassandraMappingContext}.
	 */
	public CassandraMappingContext() {

		setCustomConversions(
				new CustomConversions(StoreConversions.of(CassandraSimpleTypeHolder.HOLDER), Collections.emptyList()));
		setSimpleTypeHolder(CassandraSimpleTypeHolder.HOLDER);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#initialize()
	 */
	@Override
	public void initialize() {
		super.initialize();
		processMappingOverrides();
	}

	private void processMappingOverrides() {

		this.mapping.getEntityMappings().stream().filter(Objects::nonNull).forEach(entityMapping -> {

			Class<?> entityClass = getEntityClass(entityMapping.getEntityClassName());
			CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

			String entityTableName = entityMapping.getTableName();

			if (StringUtils.hasText(entityTableName)) {
				entity.setTableName(cqlId(entityTableName, Boolean.valueOf(entityMapping.getForceQuote())));
			}

			processMappingOverrides(entity, entityMapping);
		});
	}

	private Class<?> getEntityClass(String entityClassName) {

		try {
			return ClassUtils.forName(entityClassName, beanClassLoader);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(String.format("Unknown persistent entity name [%s]", entityClassName), e);
		}
	}

	private static void processMappingOverrides(CassandraPersistentEntity<?> entity, EntityMapping entityMapping) {

		entityMapping.getPropertyMappings()
				.forEach((key, propertyMapping) -> processMappingOverride(entity, propertyMapping));
	}

	private static void processMappingOverride(CassandraPersistentEntity<?> entity, PropertyMapping mapping) {

		CassandraPersistentProperty property = entity.getRequiredPersistentProperty(mapping.getPropertyName());

		boolean forceQuote = Boolean.valueOf(mapping.getForceQuote());

		property.setForceQuote(forceQuote);

		if (StringUtils.hasText(mapping.getColumnName())) {
			property.setColumnName(cqlId(mapping.getColumnName(), forceQuote));
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.BeanClassLoaderAware#setBeanClassLoader(java.lang.ClassLoader)
	 */
	public void setBeanClassLoader(ClassLoader beanClassLoader) {
		this.beanClassLoader = beanClassLoader;
	}

	/**
	 * Sets the {@link CustomConversions}.
	 *
	 * @param customConversions must not be {@literal null}.
	 * @since 1.5
	 */
	public void setCustomConversions(CustomConversions customConversions) {

		Assert.notNull(customConversions, "CustomConversions must not be null");

		this.customConversions = customConversions;
	}

	/**
	 * Sets the {@link Mapping}.
	 *
	 * @param mapping must not be {@literal null}.
	 */
	public void setMapping(Mapping mapping) {

		Assert.notNull(mapping, "Mapping must not be null");

		this.mapping = mapping;
	}

	/**
	 * Sets the {@link UserTypeResolver}.
	 *
	 * @param userTypeResolver must not be {@literal null}.
	 * @since 1.5
	 */
	public void setUserTypeResolver(UserTypeResolver userTypeResolver) {

		Assert.notNull(userTypeResolver, "UserTypeResolver must not be null");

		this.userTypeResolver = userTypeResolver;
	}

	/**
	 * @param verifier The verifier to set.
	 */
	public void setVerifier(CassandraPersistentEntityMetadataVerifier verifier) {
		this.verifier = verifier;
	}

	/**
	 * @return Returns the verifier.
	 */
	public CassandraPersistentEntityMetadataVerifier getVerifier() {
		return verifier;
	}

	/**
	 * Returns only {@link Table} entities.
	 *
	 * @since 1.5
	 */
	public Collection<BasicCassandraPersistentEntity<?>> getTableEntities() {
		return Collections.unmodifiableCollection(this.tableEntities);
	}

	/**
	 * Returns only those entities representing a user defined type.
	 *
	 * @since 1.5
	 */
	public Collection<CassandraPersistentEntity<?>> getUserDefinedTypeEntities() {
		return Collections.unmodifiableSet(this.userDefinedTypes);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#addPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected Optional<BasicCassandraPersistentEntity<?>> addPersistentEntity(TypeInformation<?> typeInformation) {

		// Prevent conversion types created as CassandraPersistentEntity
		Optional<BasicCassandraPersistentEntity<?>> optional = shouldCreatePersistentEntityFor(typeInformation)
				? super.addPersistentEntity(typeInformation)
				: Optional.empty();

		optional.ifPresent(entity -> {

			if (entity.isUserDefinedType()) {
				this.userDefinedTypes.add(entity);
			}
			// now do some caching of the entity

			Set<CassandraPersistentEntity<?>> entities = this.entitySetsByTableName.computeIfAbsent(entity.getTableName(),
					cqlIdentifier -> new HashSet<>());

			entities.add(entity);

			if (!entity.isUserDefinedType() && entity.isAnnotationPresent(Table.class)) {
				this.tableEntities.add(entity);
			}
		});

		return optional;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#shouldCreatePersistentEntityFor(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> typeInfo) {
		return (!this.customConversions.hasCustomWriteTarget(typeInfo.getType())
				&& super.shouldCreatePersistentEntityFor(typeInfo));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected <T> BasicCassandraPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		UserDefinedType userDefinedType = AnnotatedElementUtils.findMergedAnnotation(typeInformation.getType(),
				UserDefinedType.class);

		BasicCassandraPersistentEntity<T> entity;

		if (userDefinedType != null) {
			Assert.state(this.userTypeResolver != null, "UserTypeResolver must not be null");

			entity = new CassandraUserTypePersistentEntity<>(typeInformation, this.verifier, this.userTypeResolver);

		} else {
			entity = new BasicCassandraPersistentEntity<>(typeInformation, this.verifier);
		}

		if (this.context != null) {
			entity.setApplicationContext(this.context);
		}

		return entity;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentProperty(org.springframework.data.mapping.model.Property, org.springframework.data.mapping.model.MutablePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected CassandraPersistentProperty createPersistentProperty(Property property,
			BasicCassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {

		BasicCassandraPersistentProperty cassandraProperty = new BasicCassandraPersistentProperty(property, owner,
				simpleTypeHolder, this.userTypeResolver);

		if (this.context != null) {
			cassandraProperty.setApplicationContext(this.context);
		}

		return cassandraProperty;
	}

	/**
	 * Returns whether this mapping context has any entities mapped to the given table.
	 *
	 * @param name must not be {@literal null}.
	 * @return @return {@literal true} is this {@literal TableMetadata} is used by a mapping.
	 */
	public boolean usesTable(CqlIdentifier name) {

		Assert.notNull(name, "Table name must not be null!");

		return this.entitySetsByTableName.containsKey(name);
	}

	/**
	 * Returns whether this mapping context has any entities using the given user type.
	 *
	 * @param name must not be {@literal null}.
	 * @return {@literal true} is this {@literal UserType} is used.
	 * @since 1.5
	 */
	public boolean usesUserType(CqlIdentifier name) {

		Assert.notNull(name, "User type name must not be null!");

		return hasMappedUserType(name) || hasReferencedUserType(name);
	}

	private boolean hasReferencedUserType(CqlIdentifier identifier) {

		return getPersistentEntities().stream().flatMap(entity -> StreamSupport.stream(entity.spliterator(), false))
				.flatMap(it -> Optionals.toStream(Optional.ofNullable(it.findAnnotation(CassandraType.class))))
				.map(CassandraType::userTypeName).filter(StringUtils::hasText).map(CqlIdentifier::cqlId)
				.anyMatch(identifier::equals);
	}

	private boolean hasMappedUserType(CqlIdentifier identifier) {
		return this.userDefinedTypes.stream().map(CassandraPersistentEntity::getTableName).anyMatch(identifier::equals);
	}

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public CreateTableSpecification getCreateTableSpecificationFor(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		CreateTableSpecification specification = createTable(entity.getTableName());

		for (CassandraPersistentProperty property : entity) {

			if (!property.isCompositePrimaryKey()) {
				continue;
			}

			CassandraPersistentEntity<?> primaryKeyEntity = getRequiredPersistentEntity(property.getRawType());

			for (CassandraPersistentProperty primaryKeyProperty : primaryKeyEntity) {
				if (primaryKeyProperty.isPartitionKeyColumn()) {
					specification.partitionKeyColumn(primaryKeyProperty.getColumnName(), getDataType(primaryKeyProperty));
				} else { // it's a cluster column
					specification.clusteredKeyColumn(primaryKeyProperty.getColumnName(), getDataType(primaryKeyProperty),
							primaryKeyProperty.getPrimaryKeyOrdering());
				}
			}
		}

		for (CassandraPersistentProperty property : entity) {

			if (property.isCompositePrimaryKey()) {
				continue;
			}

			if (property.isIdProperty() || property.isPartitionKeyColumn()) {
				specification.partitionKeyColumn(property.getColumnName(),
						UserTypeUtil.potentiallyFreeze(getDataType(property)));
			} else if (property.isClusterKeyColumn()) {
				specification.clusteredKeyColumn(property.getColumnName(),
						UserTypeUtil.potentiallyFreeze(getDataType(property)), property.getPrimaryKeyOrdering());
			} else {
				specification.column(property.getColumnName(), UserTypeUtil.potentiallyFreeze(getDataType(property)));
			}
		}

		if (specification.getPartitionKeyColumns().isEmpty()) {
			throw new MappingException(String.format("No partition key columns found in entity [%s]", entity.getType()));
		}

		return specification;
	}

	/**
	 * @param entity must not be {@literal null}.
	 * @return
	 * @since 2.0
	 */
	public List<CreateIndexSpecification> getCreateIndexSpecificationsFor(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return getCreateIndexSpecifications(entity.getTableName(), entity);
	}

	private List<CreateIndexSpecification> getCreateIndexSpecifications(CqlIdentifier tableName,
			CassandraPersistentEntity<?> entity) {

		List<CreateIndexSpecification> indexes = new ArrayList<>();

		for (CassandraPersistentProperty property : entity) {
			if (property.isCompositePrimaryKey()) {
				indexes.addAll(getCreateIndexSpecifications(tableName, getRequiredPersistentEntity(property)));
			} else {
				indexes.addAll(IndexSpecificationFactory.createIndexSpecifications(property));
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

		CreateUserTypeSpecification specification = CreateUserTypeSpecification.createType(entity.getTableName());

		entity.doWithProperties((PropertyHandler<CassandraPersistentProperty>) property -> {

			// Use frozen literal to not resolve types from Cassandra.
			// At this stage, they might be not created yet.
			specification.field(property.getColumnName(),
					getDataTypeWithUserTypeFactory(property, DataTypeProvider.FrozenLiteral));
		});

		if (specification.getFields().isEmpty()) {
			throw new MappingException(String.format("No fields in user type [%s]", entity.getType()));
		}

		return specification;
	}

	/**
	 * Retrieve the data type of the property. Cassandra {@link DataType types} are determined using simple types and
	 * configured {@link org.springframework.data.convert.CustomConversions}.
	 *
	 * @param property must not be {@literal null}.
	 * @return the Cassandra {@link DataType type}.
	 * @see org.springframework.data.convert.CustomConversions
	 * @see CassandraSimpleTypeHolder
	 * @since 1.5
	 */
	public DataType getDataType(CassandraPersistentProperty property) {
		return getDataTypeWithUserTypeFactory(property, DataTypeProvider.EntityUserType);
	}

	private DataType getDataTypeWithUserTypeFactory(CassandraPersistentProperty property,
			DataTypeProvider dataTypeProvider) {

		if (property.isAnnotationPresent(CassandraType.class)) {
			return property.getDataType();
		}

		BasicCassandraPersistentEntity<?> persistentEntity = getPersistentEntity(property.getActualType());

		if (persistentEntity != null && persistentEntity.isUserDefinedType()) {

			DataType dataType = getUserDataType(property, dataTypeProvider, persistentEntity);

			if (dataType != null) {
				return dataType;
			}
		}

		return this.customConversions.getCustomWriteTarget(property.getType())
				.map(CassandraSimpleTypeHolder::getDataTypeFor).orElseGet(() -> this.customConversions
						.getCustomWriteTarget(property.getActualType()).filter(it -> !property.isMapLike()).map(it -> {

							if (property.isCollectionLike()) {

								if (List.class.isAssignableFrom(property.getType())) {
									return DataType.list(getDataTypeFor(it));
								}

								if (Set.class.isAssignableFrom(property.getType())) {
									return DataType.set(getDataTypeFor(it));
								}
							}

							return getDataTypeFor(it);
						}).orElseGet(() -> {

							if (property.isMapLike()) {

								Class<?> keyType = property.getComponentType();
								Class<?> valueType = property.getMapValueType();

								return DataType.map(getDataType(keyType, dataTypeProvider), getDataType(valueType, dataTypeProvider));
							}

							return property.getDataType();
						}));

	}

	private DataType getDataType(Class<?> type, DataTypeProvider dataTypeProvider) {

		BasicCassandraPersistentEntity<?> entity = getPersistentEntity(type);

		return entity != null && entity.isUserDefinedType() ? dataTypeProvider.getDataType(entity) : getDataType(type);
	}

	@Nullable
	private DataType getUserDataType(CassandraPersistentProperty property, DataTypeProvider dataTypeProvider,
			CassandraPersistentEntity<?> persistentEntity) {

		DataType elementType = dataTypeProvider.getDataType(persistentEntity);

		if (property.isCollectionLike()) {

			if (Set.class.isAssignableFrom(property.getType())) {
				return DataType.set(elementType);
			}

			if (List.class.isAssignableFrom(property.getType())) {
				return DataType.list(elementType);
			}
		}

		if (!property.isCollectionLike() && !property.isMapLike()) {
			return elementType;
		}

		return null;
	}

	/**
	 * Retrieve the data type based on the given {@code type}. Cassandra {@link DataType types} are determined using
	 * simple types and configured {@link org.springframework.data.convert.CustomConversions}.
	 *
	 * @param type must not be {@literal null}.
	 * @return the Cassandra {@link DataType type}.
	 * @see org.springframework.data.convert.CustomConversions
	 * @see CassandraSimpleTypeHolder
	 * @since 1.5
	 */
	public DataType getDataType(Class<?> type) {

		return this.customConversions.getCustomWriteTarget(type).map(CassandraSimpleTypeHolder::getDataTypeFor)
				.orElseGet(() -> getDataTypeFor(type));
	}

	/**
	 * @author Jens Schauder
	 * @since 1.5.1
	 */
	enum DataTypeProvider {

		EntityUserType {

			@Override
			public DataType getDataType(CassandraPersistentEntity<?> entity) {
				return entity.getUserType();
			}
		},

		FrozenLiteral {

			@Override
			public DataType getDataType(CassandraPersistentEntity<?> entity) {
				return new FrozenLiteralDataType(entity.getTableName());
			}
		};

		/**
		 * Return the data type for the {@link CassandraPersistentEntity}.
		 *
		 * @param entity must not be {@literal null}.
		 * @return the {@link DataType}.
		 */
		abstract DataType getDataType(CassandraPersistentEntity<?> entity);
	}
}
