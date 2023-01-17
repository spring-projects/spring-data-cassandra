/*
 * Copyright 2013-2023 the original author or authors.
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.CollectionFactory;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Element;
import org.springframework.data.cassandra.core.mapping.Embedded;
import org.springframework.data.cassandra.core.mapping.Embedded.OnEmpty;
import org.springframework.data.cassandra.core.mapping.EmbeddedEntityOperations;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.core.mapping.MapIdentifiable;
import org.springframework.data.cassandra.core.mapping.PersistentPropertyTranslator;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.AccessOptions;
import org.springframework.data.mapping.InstanceCreatorMetadata;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionParameterValueProvider;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.util.Predicates;
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
 * @author Frank Spitulski
 * @author neshkeev
 */
public class MappingCassandraConverter extends AbstractCassandraConverter
		implements ApplicationContextAware, BeanClassLoaderAware {

	private final Log log = LogFactory.getLog(getClass());

	private final CassandraMappingContext mappingContext;

	private CodecRegistry codecRegistry;

	private @Nullable UserTypeResolver userTypeResolver;

	private @Nullable ClassLoader beanClassLoader;

	private SpELContext spELContext;

	private final DefaultColumnTypeResolver cassandraTypeResolver;
	private final EmbeddedEntityOperations embeddedEntityOperations;
	private final SpelAwareProxyProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();

	/**
	 * Create a new {@link MappingCassandraConverter} with a {@link CassandraMappingContext}.
	 */
	public MappingCassandraConverter() {
		this(newDefaultMappingContext(new CassandraCustomConversions(Collections.emptyList())));
	}

	/**
	 * Create a new {@link MappingCassandraConverter} with the given {@link CassandraMappingContext}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingCassandraConverter(CassandraMappingContext mappingContext) {

		super(newConversionService(mappingContext));

		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");

		UserTypeResolver userTypeResolver = userTypeName -> getUserTypeResolver().resolveType(userTypeName);

		this.mappingContext = mappingContext;
		this.setCodecRegistry(mappingContext.getCodecRegistry());
		this.setCustomConversions(mappingContext.getCustomConversions());
		this.cassandraTypeResolver = new DefaultColumnTypeResolver(mappingContext, userTypeResolver, this::getCodecRegistry,
				this::getCustomConversions);
		this.embeddedEntityOperations = new EmbeddedEntityOperations(mappingContext);
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
	}

	/**
	 * Constructs a new instance of {@link ConversionContext} with various converters to convert different Cassandra value
	 * types.
	 *
	 * @return the {@link ConversionContext}.
	 * @see ConversionContext
	 */
	protected ConversionContext getConversionContext() {

		return new ConversionContext(getCustomConversions(), this::doReadRow, this::doReadTupleValue, this::doReadUdtValue,
				this::readCollectionOrArray, this::readMap, this::getPotentiallyConvertedSimpleRead);
	}

	private static ConversionService newConversionService(CassandraMappingContext mappingContext) {
		ApplicationContext applicationContext = mappingContext.getApplicationContext();
		if (applicationContext == null) return new DefaultConversionService();

		if (!applicationContext.containsBean(ConfigurableApplicationContext.CONVERSION_SERVICE_BEAN_NAME))
			return new DefaultConversionService();

		AutowireCapableBeanFactory factory = applicationContext.getAutowireCapableBeanFactory();
		if (!factory.isTypeMatch(ConfigurableApplicationContext.CONVERSION_SERVICE_BEAN_NAME, ConversionService.class))
			return new DefaultConversionService();

		final Object conversionService = applicationContext.getBean(ConfigurableApplicationContext.CONVERSION_SERVICE_BEAN_NAME);

		return conversionService instanceof ConversionService
				? (ConversionService) conversionService
				: new DefaultConversionService();
	}

	/**
	 * Constructs a new instance of a {@link MappingContext} for Cassandra.
	 *
	 * @param conversions {@link CassandraCustomConversions} object encapsulating complex type conversion logic.
	 * @return a new {@link CassandraMappingContext}.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraMappingContext
	 * @see org.springframework.data.mapping.context.MappingContext
	 */
	private static CassandraMappingContext newDefaultMappingContext(CassandraCustomConversions conversions) {

		CassandraMappingContext mappingContext = new CassandraMappingContext();

		mappingContext.setCustomConversions(conversions);
		mappingContext.afterPropertiesSet();

		return mappingContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
		this.projectionFactory.setBeanFactory(applicationContext);
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
		this.projectionFactory.setBeanClassLoader(classLoader);
	}

	private TypeCodec<Object> getCodec(CassandraPersistentProperty property) {
		return getCodecRegistry().codecFor(cassandraTypeResolver.resolve(property).getDataType());
	}

	@Override
	public ProjectionFactory getProjectionFactory() {
		return projectionFactory;
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
		return this.codecRegistry != null ? this.codecRegistry : getMappingContext().getCodecRegistry();
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
		return this.userTypeResolver != null ? this.userTypeResolver : getMappingContext().getUserTypeResolver();
	}

	@Override
	public CassandraMappingContext getMappingContext() {
		return this.mappingContext;
	}

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
				? (PersistentPropertyAccessor<S>) source
				: entity.getPropertyAccessor(source);

		return new ConvertingPropertyAccessor<>(propertyAccessor, getConversionService());
	}

	private <S> CassandraPersistentEntityParameterValueProvider newParameterValueProvider(ConversionContext context,
			CassandraPersistentEntity<S> entity, CassandraValueProvider valueProvider) {

		return new CassandraPersistentEntityParameterValueProvider(entity, valueProvider, context, null);
	}

	@SuppressWarnings("unchecked")
	private <T> Class<T> transformClassToBeanClassLoaderClass(Class<T> entity) {

		try {
			return (Class<T>) ClassUtils.forName(entity.getName(), this.beanClassLoader);
		} catch (ClassNotFoundException | LinkageError ignore) {
			return entity;
		}
	}

	@Override
	public <R> R project(EntityProjection<R, ?> projection, Row row) {

		if (!projection.isProjection()) {

			TypeInformation<?> typeToRead = projection.getMappedType().getType().isInterface() ? projection.getDomainType()
					: projection.getMappedType();
			return (R) read(typeToRead.getType(), row);
		}

		ProjectingConversionContext context = new ProjectingConversionContext(getCustomConversions(), this::doReadRow,
				this::doReadTupleValue, this::doReadUdtValue, this::readCollectionOrArray, this::readMap,
				this::getPotentiallyConvertedSimpleRead, projection);

		return doReadProjection(context, new RowValueProvider(row, new DefaultSpELExpressionEvaluator(row, spELContext)),
				projection);
	}

	@SuppressWarnings("unchecked")
	private <R> R doReadProjection(ConversionContext context, CassandraValueProvider valueProvider,
			EntityProjection<R, ?> projection) {

		CassandraPersistentEntity<?> entity = getMappingContext()
				.getRequiredPersistentEntity(projection.getActualDomainType());
		TypeInformation<?> mappedType = projection.getActualMappedType();
		CassandraPersistentEntity<R> mappedEntity = (CassandraPersistentEntity<R>) getMappingContext()
				.getPersistentEntity(mappedType);

		boolean isInterfaceProjection = mappedType.getType().isInterface();
		if (isInterfaceProjection) {

			PersistentPropertyTranslator propertyTranslator = PersistentPropertyTranslator.create(mappedEntity);
			PersistentPropertyAccessor<?> accessor = PropertyTranslatingPropertyAccessor
					.create(new MapPersistentPropertyAccessor(), propertyTranslator);

			readProperties(context, entity, valueProvider, accessor, Predicates.isTrue());
			return (R) projectionFactory.createProjection(mappedType.getType(), accessor.getBean());
		}

		// DTO projection
		if (mappedEntity == null) {
			throw new MappingException(String.format("No mapping metadata found for %s", mappedType.getType().getName()));
		}

		// create target instance, merge metadata from underlying DTO type
		PersistentPropertyTranslator propertyTranslator = PersistentPropertyTranslator.create(entity,
				Predicates.negate(CassandraPersistentProperty::hasExplicitColumnName));
		CassandraValueProvider valueProviderToUse = new TranslatingCassandraValueProvider(propertyTranslator,
				valueProvider);

		InstanceCreatorMetadata<CassandraPersistentProperty> persistenceCreator = mappedEntity
				.getInstanceCreatorMetadata();

		ParameterValueProvider<CassandraPersistentProperty> provider;
		if (persistenceCreator != null && persistenceCreator.hasParameters()) {
			SpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(valueProviderToUse.getSource(),
					spELContext);
			ParameterValueProvider<CassandraPersistentProperty> parameterValueProvider = newParameterValueProvider(context,
					entity, valueProviderToUse);
			provider = new ConverterAwareSpELExpressionParameterValueProvider(evaluator, getConversionService(),
					parameterValueProvider, context);
		} else {
			provider = NoOpParameterValueProvider.INSTANCE;
		}

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(mappedEntity);
		R instance = instantiator.createInstance(mappedEntity, provider);
		PersistentPropertyAccessor<R> accessor = mappedEntity.getPropertyAccessor(instance);

		readProperties(context, mappedEntity, valueProviderToUse, accessor, Predicates.isTrue());

		return accessor.getBean();
	}

	private Object doReadOrProject(ConversionContext context, Row row, TypeInformation<?> typeHint,
			EntityProjection<?, ?> typeDescriptor) {

		if (typeDescriptor.isProjection()) {

			CassandraValueProvider valueProvider = new RowValueProvider(row,
					new DefaultSpELExpressionEvaluator(row, this.spELContext));
			return doReadProjection(context, valueProvider, typeDescriptor);
		}

		return doReadRow(context, row, typeHint);
	}

	private Object doReadOrProject(ConversionContext context, UdtValue udtValue, TypeInformation<?> typeHint,
			EntityProjection<?, ?> typeDescriptor) {

		if (typeDescriptor.isProjection()) {

			CassandraValueProvider valueProvider = new UdtValueProvider(udtValue,
					new DefaultSpELExpressionEvaluator(udtValue, this.spELContext));
			return doReadProjection(context, valueProvider, typeDescriptor);
		}

		return doReadUdtValue(context, udtValue, typeHint);
	}

	private Object doReadOrProject(ConversionContext context, TupleValue tupleValue, TypeInformation<?> typeHint,
			EntityProjection<?, ?> typeDescriptor) {

		if (typeDescriptor.isProjection()) {

			CassandraValueProvider valueProvider = new TupleValueProvider(tupleValue,
					new DefaultSpELExpressionEvaluator(tupleValue, this.spELContext));
			return doReadProjection(context, valueProvider, typeDescriptor);
		}

		return doReadTupleValue(context, tupleValue, typeHint);
	}

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
	public <R> R readRow(Class<R> type, Row row) {

		Class<R> beanClassLoaderClass = transformClassToBeanClassLoaderClass(type);
		TypeInformation<? extends R> typeInfo = TypeInformation.of(beanClassLoaderClass);

		return doReadRow(getConversionContext(), row, typeInfo);
	}

	<S> S doReadRow(ConversionContext context, Row row, TypeInformation<? extends S> typeHint) {
		return doReadEntity(context, row, expressionEvaluator -> new RowValueProvider(row, expressionEvaluator), typeHint);
	}

	<S> S doReadTupleValue(ConversionContext context, TupleValue tupleValue, TypeInformation<? extends S> typeHint) {
		return doReadEntity(context, tupleValue,
				expressionEvaluator -> new TupleValueProvider(tupleValue, expressionEvaluator), typeHint);
	}

	<S> S doReadUdtValue(ConversionContext context, UdtValue udtValue, TypeInformation<? extends S> typeHint) {
		return doReadEntity(context, udtValue, expressionEvaluator -> new UdtValueProvider(udtValue, expressionEvaluator),
				typeHint);
	}

	private <S> S doReadEntity(ConversionContext context, Object value,
			Function<SpELExpressionEvaluator, CassandraValueProvider> valueProviderSupplier,
			TypeInformation<? extends S> typeHint) {

		SpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(value, this.spELContext);
		CassandraValueProvider valueProvider = valueProviderSupplier.apply(expressionEvaluator);

		return doReadEntity(context, valueProvider, typeHint);
	}

	/**
	 * Conversion method to materialize an object from a {@link Row}, {@link TupleValue}, or {@link UdtValue}. Can be
	 * overridden by subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param valueProvider must not be {@literal null}
	 * @param typeHint the {@link TypeInformation} to be used to unmarshall this {@link Row}.
	 * @return the converted object, will never be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	protected <S> S doReadEntity(ConversionContext context, CassandraValueProvider valueProvider,
			TypeInformation<? extends S> typeHint) {

		Class<?> rawType = typeHint.getType();
		Class<?> rawSourceType = getRawSourceType(valueProvider);

		if (rawSourceType.isAssignableFrom(rawType) && rawSourceType.isInstance(valueProvider.getSource())) {
			return (S) valueProvider.getSource();
		}

		if (getCustomConversions().hasCustomReadTarget(rawSourceType, rawType)
				|| getConversionService().canConvert(rawSourceType, rawType)) {
			return (S) getConversionService().convert(valueProvider.getSource(), rawType);
		}

		CassandraPersistentEntity<S> entity = (CassandraPersistentEntity<S>) getMappingContext()
				.getPersistentEntity(typeHint);

		if (entity == null) {
			throw new MappingException(
					String.format("Expected to read %s into type %s but didn't find a PersistentEntity for the latter",
							rawSourceType.getSimpleName(), rawType.getName()));
		}

		return doReadEntity(context, valueProvider, entity);
	}

	private static Class<?> getRawSourceType(CassandraValueProvider valueProvider) {

		if (valueProvider.getSource() instanceof Row) {
			return Row.class;
		}

		if (valueProvider.getSource() instanceof TupleValue) {
			return TupleValue.class;
		}

		if (valueProvider.getSource() instanceof UdtValue) {
			return UdtValue.class;
		}

		throw new InvalidDataAccessApiUsageException(
				String.format("Unsupported source type: %s", ClassUtils.getDescriptiveType(valueProvider.getSource())));
	}

	private <S> S doReadEntity(ConversionContext context, CassandraValueProvider valueProvider,
			CassandraPersistentEntity<S> entity) {

		InstanceCreatorMetadata<CassandraPersistentProperty> persistenceCreator = entity.getInstanceCreatorMetadata();
		ParameterValueProvider<CassandraPersistentProperty> provider;

		if (persistenceCreator != null && persistenceCreator.hasParameters()) {
			SpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(valueProvider.getSource(), spELContext);
			ParameterValueProvider<CassandraPersistentProperty> parameterValueProvider = newParameterValueProvider(context,
					entity, valueProvider);
			provider = new ConverterAwareSpELExpressionParameterValueProvider(evaluator, getConversionService(),
					parameterValueProvider, context);
		} else {
			provider = NoOpParameterValueProvider.INSTANCE;
		}

		EntityInstantiator instantiator = this.instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, provider);

		if (entity.requiresPropertyPopulation()) {
			ConvertingPropertyAccessor<S> propertyAccessor = newConvertingPropertyAccessor(instance, entity);

			readProperties(context, entity, valueProvider, propertyAccessor, isConstructorArgument(entity).negate());
			return propertyAccessor.getBean();
		}

		return instance;
	}

	private void readProperties(ConversionContext context, CassandraPersistentEntity<?> entity,
			CassandraValueProvider valueProvider, PersistentPropertyAccessor<?> propertyAccessor,
			Predicate<CassandraPersistentProperty> propertyFilter) {

		for (CassandraPersistentProperty property : entity) {

			if (!propertyFilter.test(property)) {
				continue;
			}

			ConversionContext contextToUse = context.forProperty(property.getName());

			if (property.isCompositePrimaryKey() || valueProvider.hasProperty(property) || property.isEmbedded()) {
				propertyAccessor.setProperty(property, getReadValue(contextToUse, valueProvider, property));
			}
		}
	}

	@Override
	public Object convertToColumnType(Object obj) {
		return convertToColumnType(obj, TypeInformation.of(obj.getClass()));
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
			writeInternal(newConvertingPropertyAccessor(source, entity), (Map<CqlIdentifier, Object>) sink, entity);
		} else if (sink instanceof TupleValue) {
			writeTupleValue(newConvertingPropertyAccessor(source, entity), (TupleValue) sink, entity);
		} else if (sink instanceof UdtValue) {
			writeUDTValue(newConvertingPropertyAccessor(source, entity), (UdtValue) sink, entity);
		} else {
			throw new MappingException(String.format("Unknown write target [%s]", ObjectUtils.nullSafeClassName(sink)));
		}
	}

	private void writeInternal(ConvertingPropertyAccessor<?> accessor, Map<CqlIdentifier, Object> sink,
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

				writeInternal(newConvertingPropertyAccessor(value, compositePrimaryKey), sink, compositePrimaryKey);

				continue;
			}

			Object value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug(
						String.format("doWithProperties Property.type %s, Property.value %s", property.getType().getName(), value));
			}

			if (!property.isWritable()) {
				continue;
			}

			if (value != null && property.isEmbedded()) {

				if (log.isDebugEnabled()) {
					log.debug(String.format("Mapping embedded property [%s] - [%s]", property.getRequiredColumnName(), value));
				}

				write(value, sink, embeddedEntityOperations.getEntity(property));
			} else {

				if (log.isDebugEnabled()) {
					log.debug(String.format("Adding map.entry [%s] - [%s]", property.getRequiredColumnName(), value));
				}

				sink.put(property.getRequiredColumnName(), value);
			}
		}
	}

	private void writeWhereFromObject(Object source, Where sink, CassandraPersistentEntity<?> entity) {

		Assert.notNull(source, "Id source must not be null");

		Object id = extractId(source, entity);

		Assert.notNull(id, () -> String.format("No Id value found in object %s", source));

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
			writeInternal(newConvertingPropertyAccessor(id, compositePrimaryKey), sink, compositePrimaryKey);
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
				() -> String.format("Entity [%s] is not a composite primary key", entity.getName()));

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
				log.debug(
						String.format("writeTupleValue Property.type %s, Property.value %s", property.getType().getName(), value));
			}

			if (log.isDebugEnabled()) {
				log.debug(String.format("Adding tuple value [%s] - [%s]", property.getOrdinal(), value));
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
				log.debug(String.format("writeUDTValueWhereFromObject Property.type %s, Property.value %s",
						property.getType().getName(), value));
			}

			if (log.isDebugEnabled()) {
				log.debug(String.format("Adding udt.value [%s] - [%s]", property.getRequiredColumnName(), value));
			}

			if (property.isEmbedded()) {

				if (log.isDebugEnabled()) {
					log.debug(String.format("Mapping embedded property [%s] - [%s]", property.getRequiredColumnName(), value));
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
				() -> String.format("Given instance of type [%s] is not compatible with expected type [%s]",
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
	 * {@link ColumnTypeResolver#resolve(CassandraPersistentProperty)}.
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

		TypeInformation<?> type = TypeInformation.of((Class) value.getClass());
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
	 * {@link Enum} handling or returns the value as is. Can be overridden by subclasses.
	 *
	 * @since 3.2
	 */
	protected Object getPotentiallyConvertedSimpleRead(Object value, TypeInformation<?> target) {
		return getPotentiallyConvertedSimpleRead(value, target.getType());
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is.
	 *
	 * @param value simple value to convert into a value of type {@code target}.
	 * @param target must not be {@literal null}.
	 * @return the converted value.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object getPotentiallyConvertedSimpleRead(@Nullable Object value, @Nullable Class<?> target) {

		if (value == null || target == null || ClassUtils.isAssignableValue(target, value)) {
			return value;
		}

		if (getCustomConversions().hasCustomReadTarget(value.getClass(), target)) {
			return doConvert(value, target);
		}

		if (Enum.class.isAssignableFrom(target)) {

			if (value instanceof Number) {
				int ordinal = ((Number) value).intValue();
				return target.getEnumConstants()[ordinal];
			}

			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return doConvert(value, target);
	}

	@SuppressWarnings("ConstantConditions")
	private <T> T doConvert(Object value, Class<T> target) {
		return getConversionService().convert(value, target);
	}

	private static Class<?> getCollectionType(ColumnType type) {
		return type.getType();
	}

	/**
	 * Retrieve the value to read for the given {@link CassandraPersistentProperty} from {@link CassandraValueProvider}
	 * and perform optionally a conversion of collection element types.
	 *
	 * @param context must not be {@literal null}.
	 * @param valueProvider the row.
	 * @param property the property.
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	private Object getReadValue(ConversionContext context, CassandraValueProvider valueProvider,
			CassandraPersistentProperty property) {

		if (property.isCompositePrimaryKey()) {
			return context.convert(valueProvider.getSource(), property.getTypeInformation());
		}

		if (property.isEmbedded()) {

			CassandraPersistentEntity<?> targetEntity = embeddedEntityOperations.getEntity(property);

			return isNullEmbedded(targetEntity, property, valueProvider) ? null
					: doReadEntity(context, valueProvider, targetEntity);
		}

		if (!valueProvider.hasProperty(property)) {
			return null;
		}

		Object value = valueProvider.getPropertyValue(property);

		return value == null ? null : context.convert(value, property.getTypeInformation());
	}

	/**
	 * @param entity the property domain type
	 * @param property the current property annotated with {@link Embedded}.
	 * @param valueProvider must not be {@literal null}.
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

	/**
	 * Reads the given {@link Collection} into a collection of the given {@link TypeInformation}. Will recursively resolve
	 * nested {@link List}s as well. Can be overridden by subclasses.
	 *
	 * @param source must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	protected Object readCollectionOrArray(ConversionContext context, Collection<?> source,
			TypeInformation<?> targetType) {

		Assert.notNull(targetType, "Target type must not be null");

		Class<?> collectionType = resolveCollectionType(targetType);
		Class<?> elementType = resolveElementType(targetType);

		Collection<Object> collection = targetType.getType().isArray() ? new ArrayList<>()
				: CollectionFactory.createCollection(collectionType, elementType, source.size());

		if (source.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(collection, collectionType);
		}

		TypeInformation<?> componentType = targetType.getComponentType();
		componentType = componentType == null ? TypeInformation.of(elementType) : componentType;

		for (Object element : source) {
			collection.add(context.convert(element, componentType));
		}

		return getPotentiallyConvertedSimpleRead(collection, targetType.getType());
	}

	private static Class<?> resolveCollectionType(TypeInformation<?> typeInformation) {

		Class<?> collectionType = typeInformation.getType();

		return Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;
	}

	private static Class<?> resolveElementType(TypeInformation<?> typeInformation) {

		TypeInformation<?> componentType = typeInformation.getComponentType();

		return componentType != null ? componentType.getType() : Object.class;
	}

	/**
	 * Reads the given {@link Map} into a map of the given {@link TypeInformation}. Will recursively resolve nested
	 * {@link Map}s as well. Can be overridden by subclasses.
	 *
	 * @param source must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	protected Object readMap(ConversionContext context, Map<?, ?> source, TypeInformation<?> targetType) {

		Assert.notNull(targetType, "Target type must not be null");

		TypeInformation<?> keyType = targetType.getComponentType();
		TypeInformation<?> valueType = targetType.getMapValueType();

		Class<?> rawKeyType = keyType != null ? keyType.getType() : null;

		Map<Object, Object> map = CollectionFactory.createMap(resolveMapType(targetType), rawKeyType, source.size());

		if (source.isEmpty()) {
			return map;
		}

		for (Entry<?, ?> entry : source.entrySet()) {

			Object key = entry.getKey();

			if (key != null && rawKeyType != null && !rawKeyType.isAssignableFrom(key.getClass())) {
				key = context.convert(key, keyType);
			}

			Object value = entry.getValue();

			map.put(key, context.convert(value, valueType == null ? TypeInformation.OBJECT : valueType));
		}

		return map;
	}

	private static Class<?> resolveMapType(TypeInformation<?> typeInformation) {

		Class<?> mapType = typeInformation.getType();

		return Map.class.isAssignableFrom(mapType) ? mapType : Map.class;
	}

	static Predicate<CassandraPersistentProperty> isConstructorArgument(PersistentEntity<?, ?> entity) {
		return entity::isConstructorArgument;
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

		private final ConversionContext context;

		/**
		 * Creates a new {@link ConverterAwareSpELExpressionParameterValueProvider}.
		 *
		 * @param evaluator must not be {@literal null}.
		 * @param conversionService must not be {@literal null}.
		 * @param delegate must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 */
		public ConverterAwareSpELExpressionParameterValueProvider(SpELExpressionEvaluator evaluator,
				ConversionService conversionService, ParameterValueProvider<CassandraPersistentProperty> delegate,
				ConversionContext context) {

			super(evaluator, conversionService, delegate);
			this.context = context;
		}

		@Override
		protected <T> T potentiallyConvertSpelValue(Object object, Parameter<T, CassandraPersistentProperty> parameter) {
			return (T) context.convert(object, parameter.getType());
		}
	}

	/**
	 * Conversion context holding references to simple {@link ValueConverter} and {@link ContainerValueConverter}.
	 * Entrypoint for recursive conversion of {@link Row} and other types.
	 *
	 * @since 3.2
	 */
	protected static class ConversionContext {

		final org.springframework.data.convert.CustomConversions conversions;

		final ContainerValueConverter<Row> rowConverter;

		final ContainerValueConverter<TupleValue> tupleConverter;

		final ContainerValueConverter<UdtValue> udtConverter;

		final ContainerValueConverter<Collection<?>> collectionConverter;

		final ContainerValueConverter<Map<?, ?>> mapConverter;

		final ValueConverter<Object> elementConverter;

		public ConversionContext(org.springframework.data.convert.CustomConversions conversions,
				ContainerValueConverter<Row> rowConverter, ContainerValueConverter<TupleValue> tupleConverter,
				ContainerValueConverter<UdtValue> udtConverter, ContainerValueConverter<Collection<?>> collectionConverter,
				ContainerValueConverter<Map<?, ?>> mapConverter, ValueConverter<Object> elementConverter) {
			this.conversions = conversions;
			this.rowConverter = rowConverter;
			this.tupleConverter = tupleConverter;
			this.udtConverter = udtConverter;
			this.collectionConverter = collectionConverter;
			this.mapConverter = mapConverter;
			this.elementConverter = elementConverter;
		}

		/**
		 * Obtain the {@link ConversionContext} for the property identified by the given name.
		 *
		 * @param name must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 3.4
		 */
		ConversionContext forProperty(String name) {
			return this;
		}

		/**
		 * Converts a source object into {@link TypeInformation target}.
		 *
		 * @param source must not be {@literal null}.
		 * @param typeHint must not be {@literal null}.
		 * @return the converted object.
		 */
		@SuppressWarnings("unchecked")
		public <S extends Object> S convert(Object source, TypeInformation<? extends S> typeHint) {

			Assert.notNull(typeHint, "TypeInformation must not be null");

			if (conversions.hasCustomReadTarget(source.getClass(), typeHint.getType())) {
				return (S) elementConverter.convert(source, typeHint);
			}

			if (source instanceof Collection) {

				Class<?> rawType = typeHint.getType();
				if (!Object.class.equals(rawType)) {
					if (!rawType.isArray() && !ClassUtils.isAssignable(Iterable.class, rawType)) {
						throw new MappingException(String.format(
								"Cannot convert %1$s of type %2$s into an instance of %3$s; Implement a custom Converter<%2$s, %3$s> and register it with the CustomConversions",
								source, source.getClass(), rawType));
					}
				}

				if (typeHint.isCollectionLike() || typeHint.getType().isAssignableFrom(Collection.class)) {
					return (S) collectionConverter.convert(this, (Collection<?>) source, typeHint);
				}
			}

			if (typeHint.isMap()) {
				return (S) mapConverter.convert(this, (Map<?, ?>) source, typeHint);
			}

			if (source instanceof Row) {
				return (S) rowConverter.convert(this, (Row) source, typeHint);
			}

			if (source instanceof TupleValue) {
				return (S) tupleConverter.convert(this, (TupleValue) source, typeHint);
			}

			if (source instanceof UdtValue) {
				return (S) udtConverter.convert(this, (UdtValue) source, typeHint);
			}

			return (S) elementConverter.convert(source, typeHint);
		}

		/**
		 * Converts a simple {@code source} value into {@link TypeInformation the target type}.
		 *
		 * @param <T>
		 */
		interface ValueConverter<T> {

			Object convert(T source, TypeInformation<?> typeHint);

		}

		/**
		 * Converts a container {@code source} value into {@link TypeInformation the target type}. Containers may
		 * recursively apply conversions for entities, collections, maps, etc.
		 *
		 * @param <T>
		 */
		interface ContainerValueConverter<T> {

			Object convert(ConversionContext context, T source, TypeInformation<?> typeHint);

		}

	}

	/**
	 * Cassandra-specific {@link ParameterValueProvider} considering {@link Column @Column} and {@link Element @Element}
	 * annotations on constructor parameters.
	 *
	 * @since 3.2
	 */
	class CassandraPersistentEntityParameterValueProvider implements ParameterValueProvider<CassandraPersistentProperty> {

		private final CassandraPersistentEntity<?> entity;
		private final CassandraValueProvider provider;
		private final ConversionContext context;
		private final @Nullable Object parent;

		public CassandraPersistentEntityParameterValueProvider(CassandraPersistentEntity<?> entity,
				CassandraValueProvider provider, ConversionContext context, @Nullable Object parent) {
			this.entity = entity;
			this.provider = provider;
			this.context = context;
			this.parent = parent;
		}

		@Nullable
		@SuppressWarnings("unchecked")
		public <T> T getParameterValue(Parameter<T, CassandraPersistentProperty> parameter) {

			InstanceCreatorMetadata<CassandraPersistentProperty> creatorMetadata = entity.getInstanceCreatorMetadata();

			if (creatorMetadata != null && creatorMetadata.isParentParameter(parameter)) {
				return (T) parent;
			}

			String name = parameter.getName();

			if (name == null) {
				throw new MappingException(String.format("Parameter %s does not have a name", parameter));
			}

			CassandraPersistentProperty property = getPersistentProperty(name, parameter.getType(),
					parameter.getAnnotations());

			if (property == null) {

				throw new MappingException(String.format("No property %s found on entity %s to bind constructor parameter to",
						name, entity.getType()));
			}

			return (T) getReadValue(context.forProperty(property.getName()), provider, property);
		}

		@Nullable
		private CassandraPersistentProperty getPersistentProperty(String name, TypeInformation<?> typeInformation,
				MergedAnnotations annotations) {

			CassandraPersistentProperty property = entity.getPersistentProperty(name);

			if (annotations.isPresent(Column.class) || annotations.isPresent(Element.class)) {
				return new AnnotatedCassandraConstructorProperty(
						property == null ? new CassandraConstructorProperty(name, entity, typeInformation) : property, annotations);
			}

			return property;
		}
	}

	private static class PropertyTranslatingPropertyAccessor<T> implements PersistentPropertyPathAccessor<T> {

		private final PersistentPropertyAccessor<T> delegate;
		private final PersistentPropertyTranslator propertyTranslator;

		private PropertyTranslatingPropertyAccessor(PersistentPropertyAccessor<T> delegate,
				PersistentPropertyTranslator propertyTranslator) {
			this.delegate = delegate;
			this.propertyTranslator = propertyTranslator;
		}

		static <T> PersistentPropertyAccessor<T> create(PersistentPropertyAccessor<T> delegate,
				PersistentPropertyTranslator propertyTranslator) {
			return new PropertyTranslatingPropertyAccessor<>(delegate, propertyTranslator);
		}

		@Override
		public void setProperty(PersistentProperty property, @Nullable Object value) {
			delegate.setProperty(translate(property), value);
		}

		@Override
		public Object getProperty(PersistentProperty<?> property) {
			return delegate.getProperty(translate(property));
		}

		@Override
		public T getBean() {
			return delegate.getBean();
		}

		@Override
		public void setProperty(PersistentPropertyPath<? extends PersistentProperty<?>> path, Object value,
				AccessOptions.SetOptions options) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object getProperty(PersistentPropertyPath<? extends PersistentProperty<?>> path,
				AccessOptions.GetOptions context) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setProperty(PersistentPropertyPath<? extends PersistentProperty<?>> path, Object value) {
			throw new UnsupportedOperationException();
		}

		private CassandraPersistentProperty translate(PersistentProperty<?> property) {
			return propertyTranslator.translate((CassandraPersistentProperty) property);
		}
	}

	static class TranslatingCassandraValueProvider implements CassandraValueProvider {

		private final PersistentPropertyTranslator translator;
		private final CassandraValueProvider delegate;

		public TranslatingCassandraValueProvider(PersistentPropertyTranslator translator, CassandraValueProvider delegate) {
			this.translator = translator;
			this.delegate = delegate;
		}

		@Override
		public boolean hasProperty(CassandraPersistentProperty property) {
			return delegate.hasProperty(translator.translate(property));
		}

		@Nullable
		@Override
		public <T> T getPropertyValue(CassandraPersistentProperty property) {
			return delegate.getPropertyValue(translator.translate(property));
		}

		@Override
		public Object getSource() {
			return delegate.getSource();
		}

	}

	class ProjectingConversionContext extends ConversionContext {

		private final EntityProjection<?, ?> projection;

		public ProjectingConversionContext(CustomConversions conversions, ContainerValueConverter<Row> rowConverter,
				ContainerValueConverter<TupleValue> tupleConverter, ContainerValueConverter<UdtValue> udtConverter,
				ContainerValueConverter<Collection<?>> collectionConverter, ContainerValueConverter<Map<?, ?>> mapConverter,
				ValueConverter<Object> elementConverter, EntityProjection<?, ?> projection) {
			super(conversions, (context, source, typeHint) -> doReadOrProject(context, source, typeHint, projection),
					(context, source, typeHint) -> doReadOrProject(context, source, typeHint, projection),
					(context, source, typeHint) -> doReadOrProject(context, source, typeHint, projection), collectionConverter,
					mapConverter, elementConverter);
			this.projection = projection;
		}

		@Override
		ConversionContext forProperty(String name) {

			EntityProjection<?, ?> property = projection.findProperty(name);
			if (property == null) {
				return new ConversionContext(conversions, MappingCassandraConverter.this::doReadRow,
						MappingCassandraConverter.this::doReadTupleValue, MappingCassandraConverter.this::doReadUdtValue,
						collectionConverter, mapConverter, elementConverter);
			}

			return new ProjectingConversionContext(conversions, rowConverter, tupleConverter, udtConverter,
					collectionConverter, mapConverter, elementConverter, property);
		}
	}

	static class MapPersistentPropertyAccessor implements PersistentPropertyAccessor<Map<String, Object>> {

		Map<String, Object> map = new LinkedHashMap<>();

		@Override
		public void setProperty(PersistentProperty<?> persistentProperty, Object o) {
			map.put(persistentProperty.getName(), o);
		}

		@Override
		public Object getProperty(PersistentProperty<?> persistentProperty) {
			return map.get(persistentProperty.getName());
		}

		@Override
		public Map<String, Object> getBean() {
			return map;
		}
	}

}
