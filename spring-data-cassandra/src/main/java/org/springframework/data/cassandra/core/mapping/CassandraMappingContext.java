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

import static org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder.*;
import static org.springframework.data.cql.core.CqlIdentifier.*;
import static org.springframework.data.cql.core.keyspace.CreateTableSpecification.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.mapping.UserTypeUtil.FrozenLiteralDataType;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.cql.core.CqlIdentifier;
import org.springframework.data.cql.core.keyspace.CreateTableSpecification;
import org.springframework.data.cql.core.keyspace.CreateUserTypeSpecification;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Optionals;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;

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
		implements ApplicationContextAware {

	private CassandraPersistentEntityMetadataVerifier verifier = new CompositeCassandraPersistentEntityMetadataVerifier();

	private CustomConversions customConversions;

	private Mapping mapping = new Mapping();

	private UserTypeResolver userTypeResolver;

	private ApplicationContext context;

	private ClassLoader beanClassLoader;

	// useful caches
	private final Map<Class<?>, CassandraPersistentEntity<?>> entitiesByType = new HashMap<>();
	private final Map<CqlIdentifier, Set<CassandraPersistentEntity<?>>> entitySetsByTableName = new HashMap<>();

	private final Set<BasicCassandraPersistentEntity<?>> primaryKeyEntities = new HashSet<>();
	private final Set<BasicCassandraPersistentEntity<?>> userDefinedTypes = new HashSet<>();
	private final Set<BasicCassandraPersistentEntity<?>> tableEntities = new HashSet<>();

	/**
	 * Create a new {@link CassandraMappingContext}.
	 */
	public CassandraMappingContext() {

		setCustomConversions(new CassandraCustomConversions(Collections.EMPTY_LIST));
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

	@SuppressWarnings("all")
	protected void processMappingOverrides() {

		mapping.getEntityMappings().stream()//
				.filter(entityMapping -> entityMapping != null).forEach(entityMapping -> {
					String entityClassName = entityMapping.getEntityClassName();

					try {
						Class<?> entityClass = ClassUtils.forName(entityClassName, beanClassLoader);

						CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

						String entityTableName = entityMapping.getTableName();

						if (StringUtils.hasText(entityTableName)) {
							entity.setTableName(cqlId(entityTableName, Boolean.valueOf(entityMapping.getForceQuote())));
						}

						processMappingOverrides(entity, entityMapping);

					} catch (ClassNotFoundException e) {
						throw new IllegalStateException(String.format("Unknown persistent entity name [%s]", entityClassName), e);
					}
				});
	}

	protected void processMappingOverrides(CassandraPersistentEntity<?> entity, EntityMapping entityMapping) {
		entityMapping.getPropertyMappings()
				.forEach((key, propertyMapping) -> processMappingOverride(entity, propertyMapping));
	}

	protected void processMappingOverride(CassandraPersistentEntity<?> entity, PropertyMapping mapping) {

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
	@SuppressWarnings("unused")
	public CassandraPersistentEntityMetadataVerifier getVerifier() {
		return verifier;
	}

	/**
	 * Returns only {@link Table} entities.
	 *
	 * @since 1.5
	 */
	public Collection<BasicCassandraPersistentEntity<?>> getTableEntities() {
		return Collections.unmodifiableCollection(tableEntities);
	}

	/**
	 * Returns only those entities representing a user defined type.
	 *
	 * @see #getPersistentEntities(boolean)
	 * @since 1.5
	 */
	public Collection<CassandraPersistentEntity<?>> getUserDefinedTypeEntities() {
		return Collections.unmodifiableSet(userDefinedTypes);
	}

	/**
	 * Returns all persistent entities or only non-primary-key entities.
	 *
	 * @param includePrimaryKeyTypesAndUdts If {@literal true}, returns all entities, including entities that represent
	 *          primary key types and user-defined types. If {@literal false}, returns only entities that don't represent
	 *          primary key types and no user-defined types.
	 */
	public Collection<BasicCassandraPersistentEntity<?>> getPersistentEntities(boolean includePrimaryKeyTypesAndUdts) {

		if (includePrimaryKeyTypesAndUdts) {
			return super.getPersistentEntities();
		}

		return getTableEntities();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#addPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected Optional<BasicCassandraPersistentEntity<?>> addPersistentEntity(TypeInformation<?> typeInformation) {

		// Prevent conversion types created as CassandraPersistentEntity
		Optional<BasicCassandraPersistentEntity<?>> optional = shouldCreatePersistentEntityFor(typeInformation)
				? super.addPersistentEntity(typeInformation) : Optional.empty();

		optional.ifPresent(entity -> {

			if (entity.isUserDefinedType()) {
				userDefinedTypes.add(entity);
			}
			// now do some caching of the entity

			Set<CassandraPersistentEntity<?>> entities = entitySetsByTableName.computeIfAbsent(entity.getTableName(),
					cqlIdentifier -> new HashSet<>());

			entities.add(entity);

			if (!entity.isUserDefinedType()) {
				if (entity.isCompositePrimaryKey()) {
					primaryKeyEntities.add(entity);
				}

				entity.findAnnotation(Table.class).ifPresent(table -> tableEntities.add(entity));
			}

			entitiesByType.put(entity.getType(), entity);
		});

		return optional;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#shouldCreatePersistentEntityFor(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> typeInfo) {
		return (!customConversions.hasCustomWriteTarget(typeInfo.getType())
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
			entity = new CassandraUserTypePersistentEntity<>(typeInformation, this, verifier, userTypeResolver);

		} else {
			entity = new BasicCassandraPersistentEntity<>(typeInformation, this, verifier);
		}

		if (context != null) {
			entity.setApplicationContext(context);
		}

		return entity;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.mapping.model.MutablePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected CassandraPersistentProperty createPersistentProperty(Property property,
			BasicCassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new BasicCassandraPersistentProperty(property, owner, simpleTypeHolder, userTypeResolver);
	}

	/**
	 * Returns whether this mapping context has any entities mapped to the given table.
	 *
	 * @param table must not be {@literal null}.
	 * @return @return {@literal true} is this {@literal TableMetadata} is used by a mapping.
	 */
	public boolean usesTable(TableMetadata table) {
		return entitySetsByTableName.containsKey(cqlId(table.getName()));
	}

	/**
	 * Returns whether this mapping context has any entities using the given user type.
	 *
	 * @param userType must not be {@literal null}.
	 * @return {@literal true} is this {@literal UserType} is used.
	 * @since 1.5
	 */
	public boolean usesUserType(final UserType userType) {

		CqlIdentifier identifier = CqlIdentifier.cqlId(userType.getTypeName());

		return (hasMappedUserType(identifier) || hasReferencedUserType(identifier));
	}

	private boolean hasReferencedUserType(final CqlIdentifier identifier) {

		return getPersistentEntities().stream() //
				.flatMap(PersistentEntity::getPersistentProperties) //
				.map(it -> it.findAnnotation(CassandraType.class)) //
				.filter(Optional::isPresent) //
				.flatMap(Optionals::toStream) //
				.anyMatch(it -> {
					return StringUtils.hasText(it.userTypeName()) //
							&& CqlIdentifier.cqlId(it.userTypeName()).equals(identifier);
				}); //
	}

	private boolean hasMappedUserType(CqlIdentifier identifier) {
		return userDefinedTypes.stream().map(CassandraPersistentEntity::getTableName).anyMatch(identifier::equals);
	}

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public CreateTableSpecification getCreateTableSpecificationFor(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		final CreateTableSpecification specification = createTable().name(entity.getTableName());

		entity.getPersistentProperties().filter(CassandraPersistentProperty::isCompositePrimaryKey).forEach(property -> {

			CassandraPersistentEntity<?> primaryKeyEntity = getRequiredPersistentEntity(property.getRawType());

			primaryKeyEntity.getPersistentProperties().forEach(primaryKeyProperty -> {

				if (primaryKeyProperty.isPartitionKeyColumn()) {
					specification.partitionKeyColumn(primaryKeyProperty.getColumnName(), getDataType(primaryKeyProperty));
				} else { // it's a cluster column
					specification.clusteredKeyColumn(primaryKeyProperty.getColumnName(), getDataType(primaryKeyProperty),
							primaryKeyProperty.getPrimaryKeyOrdering());
				}

			});

		});

		entity.getPersistentProperties().filter((property) -> !property.isCompositePrimaryKey()).forEach(property -> {
			if (property.isIdProperty() || property.isPartitionKeyColumn()) {
				specification.partitionKeyColumn(property.getColumnName(),
						UserTypeUtil.potentiallyFreeze(getDataType(property)));
			} else if (property.isClusterKeyColumn()) {
				specification.clusteredKeyColumn(property.getColumnName(),
						UserTypeUtil.potentiallyFreeze(getDataType(property)), property.getPrimaryKeyOrdering());
			} else {
				specification.column(property.getColumnName(), UserTypeUtil.potentiallyFreeze(getDataType(property)));
			}
		});

		if (specification.getPartitionKeyColumns().isEmpty()) {
			throw new MappingException(String.format("No partition key columns found in entity [%s]", entity.getType()));
		}

		return specification;
	}

	/**
	 * Returns a {@link CreateUserTypeSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public CreateUserTypeSpecification getCreateUserTypeSpecificationFor(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		final CreateUserTypeSpecification specification = CreateUserTypeSpecification.createType(entity.getTableName());

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

		if (property.isCompositePrimaryKey()) {
			return property.getDataType();
		}

		if (property.findAnnotation(CassandraType.class).isPresent()) {
			return property.getDataType();
		}

		Optional<BasicCassandraPersistentEntity<?>> persistentEntity = getPersistentEntity(property.getActualType());

		if (persistentEntity.filter(CassandraPersistentEntity::isUserDefinedType).isPresent()) {

			Optional<DataType> dataType = persistentEntity.map(it -> getUserDataType(property, dataTypeProvider, it));

			if (dataType.isPresent()) {
				return dataType.get();
			}
		}

		return customConversions.getCustomWriteTarget(property.getType()) //
				.map(CassandraSimpleTypeHolder::getDataTypeFor) //
				.orElseGet(() -> customConversions.getCustomWriteTarget(property.getActualType()) //
						.map(it -> {

							if (property.isCollectionLike()) {

								if (List.class.isAssignableFrom(property.getType())) {
									return DataType.list(getDataTypeFor(it));
								}

								if (Set.class.isAssignableFrom(property.getType())) {
									return DataType.set(getDataTypeFor(it));
								}
							}

							return getDataTypeFor(it);
						}).orElseGet(property::getDataType)

		);
	}

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

		return customConversions.getCustomWriteTarget(type) //
				.map(CassandraSimpleTypeHolder::getDataTypeFor) //
				.orElseGet(() -> getDataTypeFor(type));
	}

	/**
	 * Returns whether this {@link CassandraMappingContext} already contains a {@link CassandraPersistentEntity} for the
	 * given type.
	 */
	public boolean contains(Class<?> type) {
		return entitiesByType.containsKey(type);
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
		 * @return
		 */
		abstract DataType getDataType(CassandraPersistentEntity<?> entity);
	}
}
