/*
 * Copyright 2013-2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.Embedded;
import org.springframework.data.cassandra.core.mapping.Embedded.OnEmpty;
import org.springframework.data.cassandra.core.mapping.EmbeddedEntityOperations;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.core.mapping.MapIdentifiable;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionParameterValueProvider;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * {@link CassandraConverter} that uses a {@link MappingContext} to do sophisticated mapping of domain objects to
 * {@link Row}.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Antoine Toulme
 * @author John Blum
 * @author Christoph Strobl
 */
public class MappingCassandraConverter extends AbstractCassandraConverter
		implements ApplicationContextAware, BeanClassLoaderAware {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final CassandraMappingContext mappingContext;

	private CodecRegistry codecRegistry;

	private UserTypeResolver userTypeResolver;

	private @Nullable ClassLoader beanClassLoader;

	private SpELContext spELContext;

	private final DefaultColumnTypeResolver cassandraTypeResolver;
	private final EmbeddedEntityOperations embeddedEntityOperations;

	/**
	 * Create a new {@link MappingCassandraConverter} with a {@link CassandraMappingContext}.
	 */
	public MappingCassandraConverter() {

		super(newConversionService());

		CassandraCustomConversions conversions = new CassandraCustomConversions(Collections.emptyList());

		this.mappingContext = newDefaultMappingContext(conversions);
		this.codecRegistry = mappingContext.getCodecRegistry();
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
		this.cassandraTypeResolver = new DefaultColumnTypeResolver(mappingContext,
				userTypeName -> getUserTypeResolver().resolveType(userTypeName), this::getCodecRegistry,
				this::getCustomConversions);
		this.setCustomConversions(conversions);
		this.embeddedEntityOperations = new EmbeddedEntityOperations(mappingContext);
	}

	/**
	 * Create a new {@link MappingCassandraConverter} with the given {@link CassandraMappingContext}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingCassandraConverter(CassandraMappingContext mappingContext) {

		super(newConversionService());

		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");

		this.mappingContext = mappingContext;
		this.codecRegistry = mappingContext.getCodecRegistry();
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
		this.cassandraTypeResolver = new DefaultColumnTypeResolver(mappingContext,
				userTypeName -> getUserTypeResolver().resolveType(userTypeName), this::getCodecRegistry,
				this::getCustomConversions);
		this.setCustomConversions(mappingContext.getCustomConversions());
		this.embeddedEntityOperations = new EmbeddedEntityOperations(mappingContext);
	}

	private static ConversionService newConversionService() {
		return new DefaultConversionService();
	}

	private static CassandraMappingContext newDefaultMappingContext(CassandraCustomConversions conversions) {

		CassandraMappingContext mappingContext = new CassandraMappingContext();

		mappingContext.setCustomConversions(conversions);
		mappingContext.afterPropertiesSet();

		return mappingContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.BeanClassLoaderAware#setBeanClassLoader(java.lang.ClassLoader)
	 */
	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	private TypeCodec<Object> getCodec(CassandraPersistentProperty property) {
		return getCodecRegistry().codecFor(cassandraTypeResolver.resolve(property).getDataType());
	}

	/**
	 * Sets the {@link CodecRegistry}.
	 *
	 * @param codecRegistry must not be {@literal null}.
	 * @since 3.0
	 */
	public void setCodecRegistry(CodecRegistry codecRegistry) {

		Assert.notNull(codecRegistry, "CodecRegistry must not be null");

		this.codecRegistry = codecRegistry;
	}

	/**
	 * Returns the configured {@link CodecRegistry}.
	 *
	 * @return the configured {@link CodecRegistry}.
	 * @since 3.0
	 */
	@Override
	public CodecRegistry getCodecRegistry() {

		if (this.codecRegistry == null) {
			return mappingContext.getCodecRegistry();
		}

		return this.codecRegistry;
	}

	/**
	 * Sets the {@link UserTypeResolver}.
	 *
	 * @param userTypeResolver must not be {@literal null}.
	 */
	public void setUserTypeResolver(UserTypeResolver userTypeResolver) {

		Assert.notNull(userTypeResolver, "UserTypeResolver must not be null");

		this.userTypeResolver = userTypeResolver;
	}

	/**
	 * Returns the configured {@link UserTypeResolver}.
	 *
	 * @return the configured {@link UserTypeResolver}.
	 * @since 3.0
	 */
	public UserTypeResolver getUserTypeResolver() {

		if (this.userTypeResolver == null) {
			return this.mappingContext.getUserTypeResolver();
		}

		return userTypeResolver;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#getMappingContext()
	 */
	@Override
	public CassandraMappingContext getMappingContext() {
		return this.mappingContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#getColumnTypeResolver()
	 */
	@Override
	public ColumnTypeResolver getColumnTypeResolver() {
		return this.cassandraTypeResolver;
	}

	/**
	 * Create a new {@link ConvertingPropertyAccessor} for the given {@link Object source} and
	 * {@link CassandraPersistentEntity entity}.
	 *
	 * @param source {@link Object} containing the property values to access; must not be {@literal null}.
	 * @param entity {@link CassandraPersistentEntity} for the source; must not be {@literal null}.
	 * @return a new {@link ConvertingPropertyAccessor} for the given {@link Object source} and
	 *         {@link CassandraPersistentEntity entity}.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity
	 */
	@SuppressWarnings("unchecked")
	private <S> ConvertingPropertyAccessor<S> newConvertingPropertyAccessor(S source,
			CassandraPersistentEntity<?> entity) {

		PersistentPropertyAccessor<S> propertyAccessor = source instanceof PersistentPropertyAccessor
				? (PersistentPropertyAccessor) source
				: entity.getPropertyAccessor(source);

		return new ConvertingPropertyAccessor<S>(propertyAccessor, getConversionService());
	}

	private <S> PersistentEntityParameterValueProvider<CassandraPersistentProperty> newParameterValueProvider(
			CassandraPersistentEntity<S> entity, CassandraValueProvider valueProvider) {

		return new PersistentEntityParameterValueProvider<>(entity, new MappingAndConvertingValueProvider(valueProvider),
				null);
	}

	@SuppressWarnings("unchecked")
	private <T> Class<T> transformClassToBeanClassLoaderClass(Class<T> entity) {

		try {
			return (Class<T>) ClassUtils.forName(entity.getName(), this.beanClassLoader);
		} catch (ClassNotFoundException | LinkageError ignore) {
			return entity;
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.EntityReader#read(java.lang.Class, S)
	 */
	@Override
	public <R> R read(Class<R> type, Object row) {

		if (row instanceof Row) {
			return readRow(type, (Row) row);
		}

		throw new MappingException(String.format("Unknown row object [%s]", ObjectUtils.nullSafeClassName(row)));
	}

	/**
	 * Read a {@link Row} into the requested target {@link Class type}.
	 *
	 * @param type must not be {@literal null}.
	 * @param row must not be {@literal null}.
	 * @return the converted valued.
	 */
	@SuppressWarnings("unchecked")
	public <R> R readRow(Class<R> type, Row row) {

		Class<R> beanClassLoaderClass = transformClassToBeanClassLoaderClass(type);
		TypeInformation<? extends R> typeInfo = ClassTypeInformation.from(beanClassLoaderClass);
		Class<? extends R> rawType = typeInfo.getType();

		if (Row.class.isAssignableFrom(rawType)) {
			return (R) row;
		}

		if (getCustomConversions().hasCustomReadTarget(Row.class, rawType)
				|| getConversionService().canConvert(Row.class, rawType)) {

			return getConversionService().convert(row, rawType);
		}

		if (getCustomConversions().hasCustomReadTarget(com.datastax.oss.driver.api.core.cql.Row.class, rawType)
				|| getConversionService().canConvert(com.datastax.oss.driver.api.core.cql.Row.class, rawType)) {

			return getConversionService().convert(row, rawType);
		}

		if (typeInfo.isCollectionLike() || typeInfo.isMap()) {
			return getConversionService().convert(row, type);
		}

		CassandraPersistentEntity<R> persistentEntity = (CassandraPersistentEntity<R>) getMappingContext()
				.getRequiredPersistentEntity(typeInfo);

		return readEntityFromRow(persistentEntity, row);
	}

	private <S> S readEntityFromRow(CassandraPersistentEntity<S> entity, Row row) {
		return doReadEntity(entity, row, expressionEvaluator -> new RowValueProvider(row, expressionEvaluator));
	}

	private <S> S readEntityFromTuple(CassandraPersistentEntity<S> entity, TupleValue tupleValue) {

		return doReadEntity(entity, tupleValue,
				expressionEvaluator -> new TupleValueProvider(tupleValue, expressionEvaluator));
	}

	private <S> S readEntityFromUdt(CassandraPersistentEntity<S> entity, UdtValue udtValue) {

		return doReadEntity(entity, udtValue, expressionEvaluator -> new UdtValueProvider(udtValue, expressionEvaluator));
	}

	private <S> S doReadEntity(CassandraPersistentEntity<S> entity, Object value,
			Function<SpELExpressionEvaluator, CassandraValueProvider> valueProviderSupplier) {

		SpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(value, this.spELContext);
		CassandraValueProvider valueProvider = valueProviderSupplier.apply(expressionEvaluator);

		return doReadEntity(entity, valueProvider);
	}

	private <S> S doReadEntity(CassandraPersistentEntity<S> entity, CassandraValueProvider valueProvider) {

		PreferredConstructor<S, CassandraPersistentProperty> persistenceConstructor = entity.getPersistenceConstructor();
		ParameterValueProvider<CassandraPersistentProperty> provider;

		if (persistenceConstructor != null && persistenceConstructor.hasParameters()) {
			SpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(valueProvider.getSource(), spELContext);
			PersistentEntityParameterValueProvider<CassandraPersistentProperty> parameterValueProvider = newParameterValueProvider(
					entity, valueProvider);
			provider = new ConverterAwareSpELExpressionParameterValueProvider(evaluator, getConversionService(),
					parameterValueProvider);
		} else {
			provider = NoOpParameterValueProvider.INSTANCE;
		}

		EntityInstantiator instantiator = this.instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, provider);

		if (entity.requiresPropertyPopulation()) {
			ConvertingPropertyAccessor<S> propertyAccessor = newConvertingPropertyAccessor(instance, entity);

			readProperties(entity, valueProvider, propertyAccessor);
			return propertyAccessor.getBean();
		}

		return instance;
	}

	private void readProperties(CassandraPersistentEntity<?> entity, CassandraValueProvider valueProvider,
			PersistentPropertyAccessor propertyAccessor) {

		for (CassandraPersistentProperty property : entity) {
			readProperty(entity, property, valueProvider, propertyAccessor);
		}
	}

	private void readProperty(CassandraPersistentEntity<?> entity, CassandraPersistentProperty property,
			CassandraValueProvider valueProvider, PersistentPropertyAccessor propertyAccessor) {

		// if true then skip; property was set in the constructor
		if (entity.isConstructorArgument(property)) {
			return;
		}

		if (property.isCompositePrimaryKey() || valueProvider.hasProperty(property) || property.isEmbedded()) {
			propertyAccessor.setProperty(property, getReadValue(valueProvider, property));
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#convertToColumnType(java.lang.Object)
	 */
	@Override
	public Object convertToColumnType(Object obj) {
		return convertToColumnType(obj, ClassTypeInformation.from(obj.getClass()));
	}

	@Override
	public Object convertToColumnType(Object value, ColumnType columnType) {
		// noinspection ConstantConditions
		return value.getClass().isArray() ? value : getWriteValue(value, columnType);
	}

	@Override
	public void write(Object source, Object sink) {

		Assert.notNull(source, "Value must not be null");

		Class<?> beanClassLoaderClass = transformClassToBeanClassLoaderClass(source.getClass());

		CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(beanClassLoaderClass);

		write(source, sink, entity);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void write(Object source, Object sink, CassandraPersistentEntity<?> entity) {

		Assert.notNull(source, "Value must not be null");

		if (entity == null) {
			throw new MappingException("No mapping metadata found for " + source.getClass());
		}

		if (sink instanceof Where) {
			writeWhereFromObject(source, (Where) sink, entity);
		} else if (sink instanceof Map) {
			writeMapFromWrapper(newConvertingPropertyAccessor(source, entity), (Map<CqlIdentifier, Object>) sink, entity);
		} else if (sink instanceof TupleValue) {
			writeTupleValue(newConvertingPropertyAccessor(source, entity), (TupleValue) sink, entity);
		} else if (sink instanceof UdtValue) {
			writeUDTValue(newConvertingPropertyAccessor(source, entity), (UdtValue) sink, entity);
		} else {
			throw new MappingException(String.format("Unknown write target [%s]", ObjectUtils.nullSafeClassName(sink)));
		}
	}

	private void writeMapFromWrapper(ConvertingPropertyAccessor<?> accessor, Map<CqlIdentifier, Object> sink,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			if (property.isCompositePrimaryKey()) {

				if (log.isDebugEnabled()) {
					log.debug("Property is a compositeKey");
				}

				Object value = accessor.getProperty(property);

				if (value == null) {
					continue;
				}

				CassandraPersistentEntity<?> compositePrimaryKey = getMappingContext().getRequiredPersistentEntity(property);

				writeMapFromWrapper(newConvertingPropertyAccessor(value, compositePrimaryKey), sink, compositePrimaryKey);

				continue;
			}

			Object value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("doWithProperties Property.type {}, Property.value {}", property.getType().getName(), value);
			}

			if (!property.isWritable()) {
				continue;
			}

			if (value != null && property.isEmbedded()) {

				if (log.isDebugEnabled()) {
					log.debug("Mapping embedded property [{}] - [{}]", property.getRequiredColumnName(), value);
				}

				write(value, sink, embeddedEntityOperations.getEntity(property));
			} else {

				if (log.isDebugEnabled()) {
					log.debug("Adding map.entry [{}] - [{}]", property.getRequiredColumnName(), value);
				}

				sink.put(property.getRequiredColumnName(), value);
			}
		}
	}

	private void writeWhereFromObject(Object source, Where sink, CassandraPersistentEntity<?> entity) {

		Assert.notNull(source, "Id source must not be null");

		Object id = extractId(source, entity);

		Assert.notNull(id, String.format("No Id value found in object %s", source));

		CassandraPersistentProperty idProperty = entity.getIdProperty();
		CassandraPersistentProperty compositeIdProperty = null;

		if (idProperty != null && idProperty.isCompositePrimaryKey()) {
			compositeIdProperty = idProperty;
		}

		if (id instanceof MapId) {

			CassandraPersistentEntity<?> whereEntity = compositeIdProperty != null
					? getMappingContext().getRequiredPersistentEntity(compositeIdProperty)
					: entity;

			writeWhere(MapId.class.cast(id), sink, whereEntity);
			return;
		}

		if (idProperty == null) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Cannot obtain where clauses for entity [%s] using [%s]", entity.getName(), source));
		}

		if (compositeIdProperty != null) {

			if (!ClassUtils.isAssignableValue(compositeIdProperty.getType(), id)) {
				throw new InvalidDataAccessApiUsageException(
						String.format("Cannot use [%s] as composite Id for [%s]", id, entity.getName()));
			}

			CassandraPersistentEntity<?> compositePrimaryKey = getMappingContext()
					.getRequiredPersistentEntity(compositeIdProperty);

			writeWhere(newConvertingPropertyAccessor(id, compositePrimaryKey), sink, compositePrimaryKey);
			return;
		}

		Class<?> targetType = getTargetType(idProperty);

		sink.put(idProperty.getRequiredColumnName(), getPotentiallyConvertedSimpleValue(id, targetType));
	}

	@Nullable
	private Object extractId(Object source, CassandraPersistentEntity<?> entity) {

		if (ClassUtils.isAssignableValue(entity.getType(), source)) {
			return getId(source, entity);
		} else if (source instanceof MapId) {
			return source;
		} else if (source instanceof MapIdentifiable) {
			return ((MapIdentifiable) source).getMapId();
		}

		return source;
	}

	private void writeWhere(MapId id, Where sink, CassandraPersistentEntity<?> entity) {

		Assert.notNull(id, "MapId must not be null");

		for (Entry<String, Object> entry : id.entrySet()) {

			CassandraPersistentProperty persistentProperty = entity.getPersistentProperty(entry.getKey());

			if (persistentProperty == null) {
				throw new IllegalArgumentException(String.format(
						"MapId contains references [%s] that is an unknown property of [%s]", entry.getKey(), entity.getName()));
			}

			Object writeValue = getWriteValue(entry.getValue(), cassandraTypeResolver.resolve(persistentProperty));

			sink.put(persistentProperty.getRequiredColumnName(), writeValue);
		}
	}

	private void writeWhere(ConvertingPropertyAccessor<?> accessor, Where sink, CassandraPersistentEntity<?> entity) {

		Assert.isTrue(entity.isCompositePrimaryKey(),
				String.format("Entity [%s] is not a composite primary key", entity.getName()));

		for (CassandraPersistentProperty property : entity) {
			TypeCodec<Object> codec = getCodec(property);
			Object value = accessor.getProperty(property, codec.getJavaType().getRawType());
			sink.put(property.getRequiredColumnName(), value);
		}
	}

	private void writeTupleValue(ConvertingPropertyAccessor<?> propertyAccessor, TupleValue tupleValue,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			Object value = getWriteValue(property, propertyAccessor);

			if (log.isDebugEnabled()) {
				log.debug("writeTupleValue Property.type {}, Property.value {}", property.getType().getName(), value);
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding tuple value [{}] - [{}]", property.getOrdinal(), value);
			}

			TypeCodec<Object> typeCodec = getCodec(property);

			tupleValue.set(property.getRequiredOrdinal(), value, typeCodec);
		}
	}

	private void writeUDTValue(ConvertingPropertyAccessor<?> propertyAccessor, UdtValue udtValue,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			if (!property.isWritable()) {
				continue;
			}

			// value resolution
			Object value = getWriteValue(property, propertyAccessor);

			if (log.isDebugEnabled()) {
				log.debug("writeUDTValueWhereFromObject Property.type {}, Property.value {}", property.getType().getName(),
						value);
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding udt.value [{}] - [{}]", property.getRequiredColumnName(), value);
			}

			if (property.isEmbedded()) {

				if (log.isDebugEnabled()) {
					log.debug("Mapping embedded property [{}] - [{}]", property.getRequiredColumnName(), value);
				}

				if (value == null) {
					continue;
				}

				CassandraPersistentEntity<?> targetEntity = embeddedEntityOperations.getEntity(property);
				writeUDTValue(new ConvertingPropertyAccessor<>(targetEntity.getPropertyAccessor(value), getConversionService()),
						udtValue, targetEntity);

				continue;
			}

			TypeCodec<Object> typeCodec = getCodec(property);

			udtValue.set(property.getRequiredColumnName().toString(), value, typeCodec);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object getId(Object object, CassandraPersistentEntity<?> entity) {

		Assert.notNull(object, "Object instance must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		ConvertingPropertyAccessor<?> propertyAccessor = newConvertingPropertyAccessor(object, entity);

		Assert.isTrue(entity.getType().isAssignableFrom(object.getClass()),
				String.format("Given instance of type [%s] is not compatible with expected type [%s]",
						object.getClass().getName(), entity.getType().getName()));

		if (object instanceof MapIdentifiable) {
			return ((MapIdentifiable) object).getMapId();
		}

		CassandraPersistentProperty idProperty = entity.getIdProperty();

		if (idProperty != null) {
			// TODO: NullId
			return propertyAccessor.getProperty(idProperty,
					idProperty.isCompositePrimaryKey() ? (Class<Object>) idProperty.getType()
							: (Class<Object>) getTargetType(idProperty));
		}

		// if the class doesn't have an id property, then it's using MapId
		MapId id = BasicMapId.id();

		for (CassandraPersistentProperty property : entity) {

			if (!property.isPrimaryKeyColumn()) {
				continue;
			}

			id.with(property.getName(), getWriteValue(property, propertyAccessor));
		}

		return id;
	}

	/**
	 * Check custom conversions for type override or fall back to
	 * {@link #determineTargetType(CassandraPersistentProperty)}
	 *
	 * @param property
	 * @return
	 */
	private Class<?> getTargetType(CassandraPersistentProperty property) {
		return getCustomConversions().getCustomWriteTarget(property.getType())
				.orElseGet(() -> cassandraTypeResolver.resolve(property).getType());
	}

	/**
	 * Retrieve the value to write for the given {@link CassandraPersistentProperty} from
	 * {@link ConvertingPropertyAccessor} and perform optionally a conversion of collection element types.
	 *
	 * @param property the property.
	 * @param propertyAccessor the property accessor
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	private <T> T getWriteValue(CassandraPersistentProperty property, ConvertingPropertyAccessor propertyAccessor) {

		ColumnType cassandraTypeDescriptor = cassandraTypeResolver.resolve(property);

		return (T) getWriteValue(propertyAccessor.getProperty(property, cassandraTypeDescriptor.getType()),
				cassandraTypeDescriptor);
	}

	/**
	 * Retrieve the value from {@code value} applying the given {@link TypeInformation} and perform optionally a
	 * conversion of collection element types.
	 *
	 * @param value the value, may be {@literal null}.
	 * @param columnType the type information.
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	private Object getWriteValue(@Nullable Object value, ColumnType columnType) {

		if (value == null) {
			return null;
		}

		Class<?> requestedTargetType = columnType.getType();

		if (getCustomConversions().hasCustomWriteTarget(value.getClass(), requestedTargetType)) {

			Class<?> resolvedTargetType = getCustomConversions().getCustomWriteTarget(value.getClass(), requestedTargetType)
					.orElse(requestedTargetType);

			return getConversionService().convert(value, resolvedTargetType);
		}

		if (getCustomConversions().hasCustomWriteTarget(value.getClass())) {

			Class<?> resolvedTargetType = getCustomConversions().getCustomWriteTarget(value.getClass())
					.orElseThrow(() -> new IllegalStateException(String
							.format("Unable to determined custom write target for value type [%s]", value.getClass().getName())));

			return getConversionService().convert(value, resolvedTargetType);
		}

		if (getCustomConversions().isSimpleType(value.getClass())) {
			return getPotentiallyConvertedSimpleValue(value, requestedTargetType);
		}

		if (value instanceof Collection) {
			return writeCollectionInternal((Collection<Object>) value, columnType);
		}

		if (value instanceof Map) {
			return writeMapInternal((Map<Object, Object>) value, columnType);
		}

		TypeInformation<?> type = ClassTypeInformation.from((Class) value.getClass());
		TypeInformation<?> actualType = type.getRequiredActualType();
		BasicCassandraPersistentEntity<?> entity = getMappingContext().getPersistentEntity(actualType.getType());

		if (entity != null && columnType instanceof CassandraColumnType) {

			CassandraColumnType cassandraType = (CassandraColumnType) columnType;

			if (entity.isTupleType() && cassandraType.isTupleType()) {

				TupleValue tupleValue = ((TupleType) cassandraType.getDataType()).newValue();

				write(value, tupleValue, entity);

				return tupleValue;
			}

			if (entity.isUserDefinedType() && cassandraType.isUserDefinedType()) {

				UdtValue udtValue = ((UserDefinedType) cassandraType.getDataType()).newValue();

				write(value, udtValue, entity);

				return udtValue;
			}
		}

		return value;
	}

	private Object writeCollectionInternal(Collection<Object> source, ColumnType type) {

		Collection<Object> converted = CollectionFactory.createCollection(getCollectionType(type), source.size());
		ColumnType componentType = type.getRequiredComponentType();

		for (Object element : source) {
			converted.add(getWriteValue(element, componentType));
		}

		return converted;
	}

	private Object writeMapInternal(Map<Object, Object> source, ColumnType type) {

		Map<Object, Object> converted = CollectionFactory.createMap(type.getType(), source.size());

		ColumnType keyType = type.getRequiredComponentType();
		ColumnType valueType = type.getRequiredMapValueType();

		for (Entry<Object, Object> entry : source.entrySet()) {
			converted.put(getWriteValue(entry.getKey(), keyType), getWriteValue(entry.getValue(), valueType));
		}

		return converted;
	}

	/**
	 * Performs special enum handling or simply returns the value as is.
	 *
	 * @param value may be {@literal null}.
	 * @param requestedTargetType must not be {@literal null}.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraType
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	private Object getPotentiallyConvertedSimpleValue(@Nullable Object value, @Nullable Class<?> requestedTargetType) {

		if (value == null) {
			return null;
		}

		// Cassandra has no default enum handling - convert it to either a String
		// or, if requested, to a different type
		if (Enum.class.isAssignableFrom(value.getClass())) {
			if (requestedTargetType != null && !requestedTargetType.isEnum()
					&& getConversionService().canConvert(value.getClass(), requestedTargetType)) {

				return getConversionService().convert(value, requestedTargetType);
			}

			return ((Enum<?>) value).name();
		}

		return value;
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is.
	 *
	 * @param value simple value to convert into a value of type {@code target}.
	 * @param target must not be {@literal null}.
	 * @return the converted value.
	 */
	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object getPotentiallyConvertedSimpleRead(@Nullable Object value, @Nullable Class<?> target) {

		if (value == null || target == null || target.isAssignableFrom(value.getClass())) {
			return value;
		}

		if (getCustomConversions().hasCustomReadTarget(value.getClass(), target)) {
			return getConversionService().convert(value, target);
		}

		if (Enum.class.isAssignableFrom(target)) {

			if (value instanceof Number) {
				int ordinal = ((Number) value).intValue();
				return target.getEnumConstants()[ordinal];
			}

			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return getConversionService().convert(value, target);
	}

	private static Class<?> getCollectionType(ColumnType type) {
		return type.getType();
	}

	/**
	 * Retrieve the value to read for the given {@link CassandraPersistentProperty} from {@link CassandraValueProvider}
	 * and perform optionally a conversion of collection element types.
	 *
	 * @param valueProvider the row.
	 * @param property the property.
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	private Object getReadValue(CassandraValueProvider valueProvider, CassandraPersistentProperty property) {

		if (property.isCompositePrimaryKey()) {

			CassandraPersistentEntity<?> keyEntity = getMappingContext().getRequiredPersistentEntity(property);
			return doReadEntity(keyEntity, valueProvider);
		}

		if (property.isEmbedded()) {

			CassandraPersistentEntity<?> targetEntity = embeddedEntityOperations.getEntity(property);
			return isNullEmbedded(targetEntity, property, valueProvider) ? null : doReadEntity(targetEntity, valueProvider);
		}

		if (!valueProvider.hasProperty(property)) {
			return null;
		}

		Object value = valueProvider.getPropertyValue(property);
		return value == null ? null : convertReadValue(value, property.getTypeInformation());
	}

	/**
	 * @param entity the property domain type
	 * @param property the current property annotated with {@link Embedded}.
	 * @param valueProvider
	 * @return {@literal true} if the property represents a {@link Embedded.Nullable nullable embedded} entity where all
	 *         values obtainable from the given {@link CassandraValueProvider} are {@literal null}.
	 * @since 3.0
	 */
	private boolean isNullEmbedded(CassandraPersistentEntity<?> entity, CassandraPersistentProperty property,
			CassandraValueProvider valueProvider) {

		if (OnEmpty.USE_EMPTY.equals(property.getRequiredAnnotation(Embedded.class).onEmpty())) {
			return false;
		}

		for (CassandraPersistentProperty embeddedProperty : entity) {

			if (valueProvider.hasProperty(embeddedProperty) && valueProvider.getPropertyValue(embeddedProperty) != null) {
				return false;
			}
		}

		return true;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private Object convertReadValue(Object value, TypeInformation<?> typeInformation) {

		if (typeInformation.isCollectionLike() && value instanceof Collection) {
			return readCollectionOrArrayInternal((Collection<?>) value, typeInformation);
		}

		if (typeInformation.isMap() && value instanceof Map) {
			return readMapInternal((Map<Object, Object>) value, typeInformation);
		}

		if (value instanceof TupleValue) {

			BasicCassandraPersistentEntity<?> tupleEntity = getMappingContext()
					.getPersistentEntity(typeInformation.getRequiredActualType());

			if (tupleEntity != null) {
				return readEntityFromTuple(tupleEntity, (TupleValue) value);
			}
		}

		if (value instanceof UdtValue) {

			BasicCassandraPersistentEntity<?> udtEntity = getMappingContext()
					.getPersistentEntity(typeInformation.getRequiredActualType());

			if (udtEntity != null && udtEntity.isUserDefinedType()) {
				return readEntityFromUdt(udtEntity, (UdtValue) value);
			}
		}

		return getPotentiallyConvertedSimpleRead(value, typeInformation.getType());
	}

	/**
	 * Reads the given {@link Collection} into a collection of the given {@link TypeInformation}.
	 *
	 * @param source must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings({ "rawtypes" })
	private Object readCollectionOrArrayInternal(Collection<?> source, TypeInformation<?> targetType) {

		Assert.notNull(targetType, "Target type must not be null");

		Class<?> collectionType = resolveCollectionType(targetType);
		Class<?> elementType = resolveElementType(targetType);

		Collection<Object> collection = targetType.getType().isArray() ? new ArrayList<>()
				: CollectionFactory.createCollection(collectionType, elementType, source.size());

		if (source.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(collection, collectionType);
		}

		BasicCassandraPersistentEntity<?> entity = getMappingContext().getPersistentEntity(elementType);

		if (entity != null && entity.isUserDefinedType()) {
			for (Object element : source) {
				collection.add(readEntityFromUdt(entity, (UdtValue) element));
			}

		} else if (entity != null && entity.isTupleType()) {
			for (Object element : source) {
				collection.add(readEntityFromTuple(entity, (TupleValue) element));
			}
		} else {
			for (Object element : source) {
				collection.add(getPotentiallyConvertedSimpleRead(element, elementType));
			}
		}

		return getPotentiallyConvertedSimpleRead(collection, targetType.getType());
	}

	private Class<?> resolveCollectionType(TypeInformation typeInformation) {

		Class<?> collectionType = typeInformation.getType();

		return Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;
	}

	private Class<?> resolveElementType(TypeInformation typeInformation) {

		TypeInformation<?> componentType = typeInformation.getComponentType();

		return componentType != null ? componentType.getType() : Object.class;
	}

	/**
	 * Reads the given {@link Map} into a map of the given {@link TypeInformation}.
	 *
	 * @param source must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	private Object readMapInternal(Map<Object, Object> source, TypeInformation<?> targetType) {

		Assert.notNull(targetType, "Target type must not be null");

		TypeInformation<?> keyType = targetType.getComponentType();
		TypeInformation<?> valueType = targetType.getMapValueType();

		Class<?> rawKeyType = keyType != null ? keyType.getType() : null;

		Map<Object, Object> map = CollectionFactory.createMap(resolveMapType(targetType), rawKeyType, source.size());

		if (source.isEmpty()) {
			return map;
		}

		for (Entry<Object, Object> entry : source.entrySet()) {

			Object key = entry.getKey();

			if (key != null && rawKeyType != null && !rawKeyType.isAssignableFrom(key.getClass())) {
				key = convertReadValue(key, keyType);
			}

			Object value = entry.getValue();

			map.put(key, convertReadValue(value, valueType));
		}

		return map;
	}

	private Class<?> resolveMapType(TypeInformation<?> typeInformation) {

		Class<?> mapType = typeInformation.getType();

		return Map.class.isAssignableFrom(mapType) ? mapType : Map.class;
	}

	enum NoOpParameterValueProvider implements ParameterValueProvider<CassandraPersistentProperty> {

		INSTANCE;

		@Override
		public <T> T getParameterValue(Parameter<T, CassandraPersistentProperty> parameter) {
			return null;
		}
	}

	/**
	 * Extension of {@link SpELExpressionParameterValueProvider} to recursively trigger value conversion on the raw
	 * resolved SpEL value.
	 */
	private class ConverterAwareSpELExpressionParameterValueProvider
			extends SpELExpressionParameterValueProvider<CassandraPersistentProperty> {

		/**
		 * Creates a new {@link ConverterAwareSpELExpressionParameterValueProvider}.
		 *
		 * @param evaluator must not be {@literal null}.
		 * @param conversionService must not be {@literal null}.
		 * @param delegate must not be {@literal null}.
		 */
		public ConverterAwareSpELExpressionParameterValueProvider(SpELExpressionEvaluator evaluator,
				ConversionService conversionService, ParameterValueProvider<CassandraPersistentProperty> delegate) {

			super(evaluator, conversionService, delegate);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.SpELExpressionParameterValueProvider#potentiallyConvertSpelValue(java.lang.Object, org.springframework.data.mapping.PreferredConstructor.Parameter)
		 */
		@Override
		protected <T> T potentiallyConvertSpelValue(Object object, Parameter<T, CassandraPersistentProperty> parameter) {
			return (T) convertReadValue(object, parameter.getType());
		}
	}

	/**
	 * {@link CassandraRowValueProvider} that delegates reads to {@link CassandraValueProvider} applying mapping and
	 * custom conversion from {@link MappingCassandraConverter}.
	 *
	 * @author Mark Paluch
	 * @since 1.5.1
	 */
	class MappingAndConvertingValueProvider implements CassandraValueProvider {

		private final CassandraValueProvider parent;

		public MappingAndConvertingValueProvider(CassandraValueProvider parent) {
			this.parent = parent;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.convert.CassandraValueProvider#hasProperty(org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty)
		 */
		@Override
		public boolean hasProperty(CassandraPersistentProperty property) {
			return this.parent.hasProperty(property);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
		 */
		@Nullable
		@Override
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(CassandraPersistentProperty property) {
			return (T) getReadValue(this.parent, property);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.convert.CassandraValueProvider#getSource()
		 */
		@Override
		public Object getSource() {
			return parent.getSource();
		}
	}
}
