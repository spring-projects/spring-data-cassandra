/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.convert.CustomConversions;
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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Default implementation of a {@link MappingContext} for Cassandra using {@link CassandraPersistentEntity} and
 * {@link CassandraPersistentProperty} as primary abstractions.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author John Blum
 * @author Jens Schauder
 * @author Vagif Zeynalov
 */
public class CassandraMappingContext
		extends AbstractMappingContext<BasicCassandraPersistentEntity<?>, CassandraPersistentProperty>
		implements ApplicationContextAware, BeanClassLoaderAware {

	private @Nullable ApplicationContext applicationContext;

	private CassandraPersistentEntityMetadataVerifier verifier = new CompositeCassandraPersistentEntityMetadataVerifier();

	private @Nullable ClassLoader beanClassLoader;

	private @Deprecated CustomConversions customConversions = new CassandraCustomConversions(Collections.emptyList());

	private Mapping mapping = new Mapping();

	private NamingStrategy namingStrategy = NamingStrategy.INSTANCE;

	private @Deprecated @Nullable UserTypeResolver userTypeResolver;

	private @Deprecated CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

	// caches
	private final Map<CqlIdentifier, Set<CassandraPersistentEntity<?>>> entitySetsByTableName = new ConcurrentHashMap<>();

	private final Set<BasicCassandraPersistentEntity<?>> tableEntities = ConcurrentHashMap.newKeySet();

	private final Set<BasicCassandraPersistentEntity<?>> userDefinedTypes = ConcurrentHashMap.newKeySet();

	/**
	 * Create a new {@link CassandraMappingContext}.
	 */
	public CassandraMappingContext() {
		setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
	}

	/**
	 * Create a new {@link CassandraMappingContext} given {@link UserTypeResolver} and {@link TupleTypeFactory}.
	 *
	 * @param userTypeResolver must not be {@literal null}.
	 * @param tupleTypeFactory must not be {@literal null}.
	 * @since 2.1
	 * @deprecated since 3.0, {@link UserTypeResolver} and {@link TupleTypeFactory} no longer required here as high-level
	 *             type resolution went into {@link org.springframework.data.cassandra.core.convert.CassandraConverter}.
	 */
	@Deprecated
	public CassandraMappingContext(UserTypeResolver userTypeResolver, TupleTypeFactory tupleTypeFactory) {

		setUserTypeResolver(userTypeResolver);
		setTupleTypeFactory(tupleTypeFactory);
		setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
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
	private void processMappingOverrides() {

		this.mapping.getEntityMappings().stream().filter(Objects::nonNull).forEach(entityMapping -> {

			Class<?> entityClass = getEntityClass(entityMapping.getEntityClassName());

			CassandraPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

			String entityTableName = entityMapping.getTableName();

			if (StringUtils.hasText(entityTableName)) {
				entity.setTableName(IdentifierFactory.create(entityTableName, Boolean.valueOf(entityMapping.getForceQuote())));
			}

			processMappingOverrides(entity, entityMapping);
		});
	}

	private Class<?> getEntityClass(String entityClassName) {

		try {
			return ClassUtils.forName(entityClassName, this.beanClassLoader);
		} catch (ClassNotFoundException cause) {
			throw new IllegalStateException(String.format("Unknown persistent entity type name [%s]", entityClassName),
					cause);
		}
	}

	private static void processMappingOverrides(CassandraPersistentEntity<?> entity, EntityMapping entityMapping) {

		entityMapping.getPropertyMappings()
				.forEach((key, propertyMapping) -> processMappingOverride(entity, propertyMapping));
	}

	private static void processMappingOverride(CassandraPersistentEntity<?> entity, PropertyMapping mapping) {

		CassandraPersistentProperty property = entity.getRequiredPersistentProperty(mapping.getPropertyName());

		boolean forceQuote = Boolean.parseBoolean(mapping.getForceQuote());

		property.setForceQuote(forceQuote);

		if (StringUtils.hasText(mapping.getColumnName())) {
			property.setColumnName(IdentifierFactory.create(mapping.getColumnName(), forceQuote));
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
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
	 * @deprecated since 3.0. Use custom conversion through
	 *             {@link org.springframework.data.cassandra.core.convert.MappingCassandraConverter}.
	 */
	@Deprecated
	public void setCustomConversions(CustomConversions customConversions) {

		Assert.notNull(customConversions, "CustomConversions must not be null");

		this.customConversions = customConversions;
	}

	/**
	 * @deprecated since 3.0. Use custom conversion through
	 * {@link org.springframework.data.cassandra.core.convert.MappingCassandraConverter}.
	 */
	@Deprecated
	public CustomConversions getCustomConversions() {
		return customConversions;
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

	/**
	 * Sets the {@link CodecRegistry}.
	 *
	 * @param codecRegistry must not be {@literal null}.
	 * @since 2.2
	 * @deprecated since 3.0. Set {@link CodecRegistry} directly on
	 *             {@link org.springframework.data.cassandra.core.convert.CassandraConverter}.
	 */
	@Deprecated
	public void setCodecRegistry(CodecRegistry codecRegistry) {

		Assert.notNull(codecRegistry, "CodecRegistry must not be null");

		this.codecRegistry = codecRegistry;
	}

	/**
	 * @deprecated since 3.0. Retrieve {@link CodecRegistry} directly from
	 * {@link org.springframework.data.cassandra.core.convert.CassandraConverter}.
	 */
	@Deprecated
	public CodecRegistry getCodecRegistry() {
		return this.codecRegistry;
	}

	/**
	 * Set the {@link NamingStrategy} to use.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 * @since 3.0
	 */
	public void setNamingStrategy(NamingStrategy namingStrategy) {

		Assert.notNull(namingStrategy, "NamingStrategy must not be null");

		this.namingStrategy = namingStrategy;
	}

	/**
	 * Sets the {@link TupleTypeFactory}.
	 *
	 * @param tupleTypeFactory must not be {@literal null}.
	 * @since 2.1
	 * @deprecated since 3.0. Tuple type creation uses
	 *             {@link com.datastax.oss.driver.api.core.type.DataTypes#tupleOf(DataType...)}
	 */
	@Deprecated
	public void setTupleTypeFactory(TupleTypeFactory tupleTypeFactory) {}

	/**
	 * Sets the {@link UserTypeResolver}.
	 *
	 * @param userTypeResolver must not be {@literal null}.
	 * @since 1.5
	 * @deprecated since 3.0. Set {@link UserTypeResolver} directly on
	 *             {@link org.springframework.data.cassandra.core.convert.CassandraConverter}.
	 */
	@Deprecated
	public void setUserTypeResolver(UserTypeResolver userTypeResolver) {

		Assert.notNull(userTypeResolver, "UserTypeResolver must not be null");

		this.userTypeResolver = userTypeResolver;
	}

	/**
	 * @deprecated since 3.0. Retrieve {@link UserTypeResolver} directly from
	 * {@link org.springframework.data.cassandra.core.convert.CassandraConverter}.
	 */
	@Nullable
	@Deprecated
	public UserTypeResolver getUserTypeResolver() {
		return this.userTypeResolver;
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
		return this.verifier;
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
					cqlIdentifier -> ConcurrentHashMap.newKeySet());

			entities.add(entity);

			if (!entity.isUserDefinedType() && !entity.isTupleType() && entity.isAnnotationPresent(Table.class)) {
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
		return !this.customConversions.hasCustomWriteTarget(typeInfo.getType())
				&& super.shouldCreatePersistentEntityFor(typeInfo);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected <T> BasicCassandraPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		BasicCassandraPersistentEntity<T> entity = isUserDefinedType(typeInformation)
				? new CassandraUserTypePersistentEntity<>(typeInformation, getVerifier())
				: isTuple(typeInformation)
				? new BasicCassandraPersistentTupleEntity<>(typeInformation)
				: new BasicCassandraPersistentEntity<>(typeInformation, getVerifier());

		entity.setNamingStrategy(this.namingStrategy);
		Optional.ofNullable(this.applicationContext).ifPresent(entity::setApplicationContext);

		return entity;
	}

	private boolean isTuple(TypeInformation<?> typeInformation) {
		return AnnotatedElementUtils.hasAnnotation(typeInformation.getType(), Tuple.class);
	}

	private boolean isUserDefinedType(TypeInformation<?> typeInformation) {
		return AnnotatedElementUtils.hasAnnotation(typeInformation.getType(), UserDefinedType.class);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentProperty(org.springframework.data.mapping.model.Property, org.springframework.data.mapping.model.MutablePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected CassandraPersistentProperty createPersistentProperty(Property property,
			BasicCassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {

		BasicCassandraPersistentProperty persistentProperty = owner.isTupleType()
				? new BasicCassandraPersistentTupleProperty(property, owner, simpleTypeHolder)
				: new CachingCassandraPersistentProperty(property, owner, simpleTypeHolder);

		persistentProperty.setNamingStrategy(this.namingStrategy);
		Optional.ofNullable(this.applicationContext).ifPresent(persistentProperty::setApplicationContext);

		return persistentProperty;
	}

	/**
	 * Returns whether this mapping context has any entities mapped to the given table.
	 *
	 * @param name must not be {@literal null}.
	 * @return @return {@literal true} is this {@literal TableMetadata} is used by a mapping.
	 */
	public boolean usesTable(CqlIdentifier name) {

		Assert.notNull(name, "Table name must not be null");

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

		Assert.notNull(name, "User type name must not be null");

		return hasMappedUserType(name) || hasReferencedUserType(name);
	}

	private boolean hasMappedUserType(CqlIdentifier identifier) {
		return this.userDefinedTypes.stream().map(CassandraPersistentEntity::getTableName).anyMatch(identifier::equals);
	}

	private boolean hasReferencedUserType(CqlIdentifier identifier) {

		return getPersistentEntities().stream().flatMap(entity -> StreamSupport.stream(entity.spliterator(), false))
				.flatMap(it -> Optionals.toStream(Optional.ofNullable(it.findAnnotation(CassandraType.class))))
				.map(CassandraType::userTypeName)
				.filter(StringUtils::hasText)
				.map(CqlIdentifier::fromCql)
				.anyMatch(identifier::equals);
	}
}
