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
package org.springframework.data.cassandra.core.convert;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.core.mapping.MapIdentifiable;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

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
 */
public class MappingCassandraConverter extends AbstractCassandraConverter
		implements CassandraConverter, ApplicationContextAware, BeanClassLoaderAware {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final CassandraMappingContext mappingContext;

	private @Nullable ClassLoader beanClassLoader;

	private SpELContext spELContext;

	/**
	 * Create a new {@link MappingCassandraConverter} with a {@link CassandraMappingContext}.
	 */
	public MappingCassandraConverter() {
		this(createMappingContext());
	}

	/**
	 * Create a new {@link MappingCassandraConverter} with the given {@link CassandraMappingContext}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingCassandraConverter(CassandraMappingContext mappingContext) {

		super(new DefaultConversionService());

		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");

		this.mappingContext = mappingContext;
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
	}

	private static CassandraMappingContext createMappingContext() {

		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.setCustomConversions(new CassandraCustomConversions(Collections.emptyList()));

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

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#getMappingContext()
	 */
	@Override
	public CassandraMappingContext getMappingContext() {
		return mappingContext;
	}

	/**
	 * Read a {@link Row} into the requested target {@code type}.
	 *
	 * @param type must not be {@literal null}.
	 * @param row must not be {@literal null}.
	 * @return the converted valued.
	 */
	@Nullable
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

		if (typeInfo.isCollectionLike() || typeInfo.isMap()) {
			return getConversionService().convert(row, type);
		}

		CassandraPersistentEntity<R> persistentEntity = (CassandraPersistentEntity<R>) getMappingContext()
				.getRequiredPersistentEntity(typeInfo);

		return readEntityFromRow(persistentEntity, row);
	}

	protected <S> S readEntityFromRow(CassandraPersistentEntity<S> entity, Row row) {

		DefaultSpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(row, spELContext);

		BasicCassandraRowValueProvider rowValueProvider = new BasicCassandraRowValueProvider(row, expressionEvaluator);

		PersistentEntityParameterValueProvider<CassandraPersistentProperty> parameterValueProvider = new PersistentEntityParameterValueProvider<>(
				entity, new MappingAndConvertingValueProvider(rowValueProvider), null);

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterValueProvider);

		readPropertiesFromRow(entity, rowValueProvider, getConvertingAccessor(instance, entity));

		return instance;
	}

	protected <S> S readEntityFromUdt(CassandraPersistentEntity<S> entity, UDTValue udtValue) {

		DefaultSpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(udtValue, spELContext);

		CassandraUDTValueProvider valueProvider = new CassandraUDTValueProvider(udtValue, CodecRegistry.DEFAULT_INSTANCE,
				expressionEvaluator);

		PersistentEntityParameterValueProvider<CassandraPersistentProperty> parameterValueProvider = getParameterValueProvider(
				entity, valueProvider);

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterValueProvider);

		readProperties(entity, valueProvider, getConvertingAccessor(instance, entity));

		return instance;
	}

	private <S> PersistentEntityParameterValueProvider<CassandraPersistentProperty> getParameterValueProvider(
			CassandraPersistentEntity<S> entity, CassandraValueProvider valueProvider) {
		return new PersistentEntityParameterValueProvider<>(entity, new MappingAndConvertingValueProvider(valueProvider),
				null);
	}

	protected void readPropertiesFromRow(CassandraPersistentEntity<?> entity, CassandraRowValueProvider row,
			PersistentPropertyAccessor propertyAccessor) {

		readProperties(entity, row, propertyAccessor);
	}

	protected void readProperties(CassandraPersistentEntity<?> entity, CassandraValueProvider valueProvider,
			PersistentPropertyAccessor propertyAccessor) {

		for (CassandraPersistentProperty property : entity) {
			MappingCassandraConverter.this.readProperty(entity, property, valueProvider, propertyAccessor);
		}
	}

	protected void readProperty(CassandraPersistentEntity<?> entity, CassandraPersistentProperty property,
			CassandraValueProvider valueProvider, PersistentPropertyAccessor propertyAccessor) {

		// if true then skip; property was set in constructor
		if (entity.isConstructorArgument(property)) {
			return;
		}

		if (property.isCompositePrimaryKey()) {

			CassandraPersistentEntity<?> keyEntity = mappingContext.getRequiredPersistentEntity(property);
			Object key = propertyAccessor.getProperty(property);

			if (key == null) {
				key = instantiatePrimaryKey(keyEntity, property, valueProvider);
			}

			// now recurse on using the key this time
			readProperties(keyEntity, valueProvider, getConvertingAccessor(key, keyEntity));

			// now that the key's properties have been populated, set the key property on the entity
			propertyAccessor.setProperty(property, key);

			return;
		}

		if (!valueProvider.hasProperty(property)) {
			return;
		}

		propertyAccessor.setProperty(property, getReadValue(valueProvider, property));
	}

	@SuppressWarnings("unused")
	protected Object instantiatePrimaryKey(CassandraPersistentEntity<?> entity, CassandraPersistentProperty keyProperty,
			CassandraValueProvider propertyProvider) {

		return instantiators.getInstantiatorFor(entity).createInstance(entity,
				getParameterValueProvider(entity, propertyProvider));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.EntityReader#read(java.lang.Class, S)
	 */
	@Override
	public <R> R read(Class<R> type, Object row) {

		if (row instanceof Row) {
			return readRow(type, (Row) row);
		}

		throw new MappingException("Unknown row object " + ObjectUtils.nullSafeClassName(row));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#convertToColumnType(java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Object convertToColumnType(Object obj) {
		return convertToColumnType(obj, ClassTypeInformation.from(obj.getClass()));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#convertToColumnType(java.lang.Object, org.springframework.data.util.TypeInformation)
	 */
	@Override
	public Object convertToColumnType(Object value, TypeInformation<?> typeInformation) {

		Assert.notNull(value, "Value must not be null");
		Assert.notNull(typeInformation, "TypeInformation must not be null");

		// noinspection ConstantConditions
		return value.getClass().isArray() ? value : getWriteValue(value, typeInformation);
	}

	@Override
	public void write(Object source, Object sink) {

		Assert.notNull(source, "Value must not be null");

		Class<?> beanClassLoaderClass = transformClassToBeanClassLoaderClass(source.getClass());
		CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(beanClassLoaderClass);

		write(source, sink, entity);
	}

	@SuppressWarnings("unchecked")
	private <T> Class<T> transformClassToBeanClassLoaderClass(Class<T> entity) {
		try {
			return (Class<T>) ClassUtils.forName(entity.getName(), beanClassLoader);
		} catch (ClassNotFoundException | LinkageError e) {
			return entity;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void write(Object source, Object sink, CassandraPersistentEntity<?> entity) {

		Assert.notNull(source, "Value must not be null");

		if (entity == null) {
			throw new MappingException("No mapping metadata found for " + source.getClass());
		}

		if (sink instanceof Map) {
			writeMapFromWrapper(getConvertingAccessor(source, entity), (Map<String, Object>) sink, entity);
		} else if (sink instanceof Insert) {
			writeInsertFromObject(source, (Insert) sink, entity);
		} else if (sink instanceof Update) {
			writeUpdateFromObject(source, (Update) sink, entity);
		} else if (sink instanceof Select.Where) {
			writeSelectWhereFromObject(source, (Select.Where) sink, entity);
		} else if (sink instanceof Delete.Where) {
			writeDeleteWhereFromObject(source, (Delete.Where) sink, entity);
		} else if (sink instanceof UDTValue) {
			writeUDTValueWhereFromObject(getConvertingAccessor(source, entity), (UDTValue) sink, entity);
		} else {
			throw new MappingException("Unknown write target " + sink.getClass().getName());
		}
	}

	protected void writeInsertFromObject(Object object, Insert insert, CassandraPersistentEntity<?> entity) {
		writeInsertFromWrapper(getConvertingAccessor(object, entity), insert, entity);
	}

	private void writeMapFromWrapper(ConvertingPropertyAccessor accessor, Map<String, Object> insert,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			Object value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("doWithProperties Property.type {}, Property.value {}", property.getType().getName(), value);
			}

			if (property.isCompositePrimaryKey()) {

				if (log.isDebugEnabled()) {
					log.debug("Property is a compositeKey");
				}

				if (value == null) {
					continue;
				}

				CassandraPersistentEntity<?> compositePrimaryKey = mappingContext.getRequiredPersistentEntity(property);
				writeMapFromWrapper(getConvertingAccessor(value, compositePrimaryKey), insert, compositePrimaryKey);

				continue;
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding map.entry [{}] - [{}]", property.getColumnName().toCql(), value);
			}

			insert.put(property.getColumnName().toCql(), value);
		}
	}

	protected void writeInsertFromWrapper(final ConvertingPropertyAccessor accessor, final Insert insert,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			Object value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("doWithProperties Property.type {}, Property.value {}", property.getType().getName(), value);
			}

			if (property.isCompositePrimaryKey()) {

				if (log.isDebugEnabled()) {
					log.debug("Property is a compositeKey");
				}

				if (value == null) {
					continue;
				}

				CassandraPersistentEntity<?> compositePrimaryKey = mappingContext.getRequiredPersistentEntity(property);
				writeInsertFromWrapper(getConvertingAccessor(value, compositePrimaryKey), insert, compositePrimaryKey);

				continue;
			}

			if (value == null) {
				continue;
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding insert.value [{}] - [{}]", property.getColumnName().toCql(), value);
			}

			insert.value(property.getColumnName().toCql(), value);
		}
	}

	protected void writeUpdateFromObject(final Object object, final Update update, CassandraPersistentEntity<?> entity) {
		writeUpdateFromWrapper(getConvertingAccessor(object, entity), update, entity);
	}

	protected void writeUpdateFromWrapper(final ConvertingPropertyAccessor accessor, final Update update,
			final CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			Object value = getWriteValue(property, accessor);

			if (property.isCompositePrimaryKey()) {

				CassandraPersistentEntity<?> compositePrimaryKey = mappingContext.getRequiredPersistentEntity(property);

				if (value == null) {
					continue;
				}

				writeUpdateFromWrapper(getConvertingAccessor(value, compositePrimaryKey), update, compositePrimaryKey);

				continue;
			}

			if (isPrimaryKeyPart(property)) {
				update.where(QueryBuilder.eq(property.getColumnName().toCql(), value));
			} else {
				update.with(QueryBuilder.set(property.getColumnName().toCql(), value));
			}
		}
	}

	protected void writeSelectWhereFromObject(Object object, Select.Where where, CassandraPersistentEntity<?> entity) {
		getWhereClauses(object, entity).forEach(where::and);
	}

	protected void writeDeleteWhereFromObject(Object object, Delete.Where where, CassandraPersistentEntity<?> entity) {
		getWhereClauses(object, entity).forEach(where::and);
	}

	protected void writeUDTValueWhereFromObject(ConvertingPropertyAccessor accessor, UDTValue udtValue,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			Object value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("writeUDTValueWhereFromObject Property.type {}, Property.value {}", property.getType().getName(),
						value);
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding udt.value [{}] - [{}]", property.getColumnName().toCql(), value);
			}

			TypeCodec<Object> typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(getMappingContext().getDataType(property));

			udtValue.set(property.getColumnName().toCql(), value, typeCodec);
		}
	}

	@SuppressWarnings("unchecked")
	private Collection<Clause> getWhereClauses(Object source, CassandraPersistentEntity<?> entity) {

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
					? mappingContext.getRequiredPersistentEntity(compositeIdProperty)
					: entity;

			return getWhereClauses(MapId.class.cast(id), whereEntity);
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

			CassandraPersistentEntity<?> compositePrimaryKey = mappingContext
					.getRequiredPersistentEntity(compositeIdProperty);

			return getWhereClauses(getConvertingAccessor(id, compositePrimaryKey), compositePrimaryKey);
		}

		Class<?> targetType = getTargetType(idProperty);

		return Collections.singleton(
				QueryBuilder.eq(idProperty.getColumnName().toCql(), getPotentiallyConvertedSimpleValue(id, targetType)));
	}

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

	private Collection<Clause> getWhereClauses(final ConvertingPropertyAccessor accessor,
			CassandraPersistentEntity<?> entity) {

		Assert.isTrue(entity.isCompositePrimaryKey(),
				String.format("Entity [%s] is not a composite primary key", entity.getName()));

		Collection<Clause> clauses = new ArrayList<>();

		for (CassandraPersistentProperty property : entity) {
			TypeCodec<Object> codec = getCodec(property);
			Object value = accessor.getProperty(property, codec.getJavaType().getRawType());
			clauses.add(QueryBuilder.eq(property.getColumnName().toCql(), value));
		}

		return clauses;
	}

	private Collection<Clause> getWhereClauses(MapId id, CassandraPersistentEntity<?> entity) {

		Assert.notNull(id, "MapId must not be null");

		Collection<Clause> clauses = new ArrayList<>();

		for (Entry<String, Object> entry : id.entrySet()) {

			CassandraPersistentProperty persistentProperty = entity.getPersistentProperty(entry.getKey());

			if (persistentProperty == null) {
				throw new IllegalArgumentException(String.format(
						"MapId contains references [%s] that is an unknown property of [%s]", entry.getKey(), entity.getName()));
			}

			Object writeValue = getWriteValue(entry.getValue(), persistentProperty.getTypeInformation());

			clauses.add(QueryBuilder.eq(persistentProperty.getColumnName().toCql(), writeValue));
		}

		return clauses;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object getId(Object object, CassandraPersistentEntity<?> entity) {

		Assert.notNull(object, "Object instance must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		ConvertingPropertyAccessor propertyAccessor = getConvertingAccessor(object, entity);

		Assert.isTrue(entity.getType().isAssignableFrom(object.getClass()),
				String.format("Given instance of type [%s] is not of compatible expected type [%s]",
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
		final MapId id = BasicMapId.id();

		for (CassandraPersistentProperty property : entity) {

			if (!property.isPrimaryKeyColumn()) {
				continue;
			}

			id.with(property.getName(), getWriteValue(property, propertyAccessor));
		}

		return id;
	}

	/**
	 * Create a new {@link ConvertingPropertyAccessor} for the given source and entity.
	 *
	 * @param source must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return a new {@link ConvertingPropertyAccessor} for the given source and entity.
	 */
	private ConvertingPropertyAccessor getConvertingAccessor(Object source, CassandraPersistentEntity<?> entity) {

		PersistentPropertyAccessor propertyAccessor = (source instanceof PersistentPropertyAccessor
				? (PersistentPropertyAccessor) source
				: entity.getPropertyAccessor(source));

		return new ConvertingPropertyAccessor(propertyAccessor, getConversionService());
	}

	/**
	 * Returns whether the property is part of the primary key.
	 *
	 * @param property {@link CassandraPersistentProperty} to evaluate.
	 * @return a boolean value indicating whether the given property is party of a primary key.
	 */
	private boolean isPrimaryKeyPart(CassandraPersistentProperty property) {
		return (property.isCompositePrimaryKey() || property.isPrimaryKeyColumn() || property.isIdProperty());
	}

	private Class<?> getTargetType(CassandraPersistentProperty property) {

		return getCustomConversions().getCustomWriteTarget(property.getType()).orElseGet(() -> {

			if (property.isAnnotationPresent(CassandraType.class)) {
				return getPropertyTargetType(property);
			}

			if (property.isCompositePrimaryKey() || getCustomConversions().isSimpleType(property.getType())
					|| property.isCollectionLike()) {

				return property.getType();
			}

			return getPropertyTargetType(property);
		});

	}

	private Class<?> getPropertyTargetType(CassandraPersistentProperty property) {

		DataType dataType = getMappingContext().getDataType(property);

		if (dataType instanceof UserType) {
			return property.getType();
		}

		TypeCodec<Object> codec = CodecRegistry.DEFAULT_INSTANCE.codecFor(getMappingContext().getDataType(property));

		return codec.getJavaType().getRawType();
	}

	/**
	 * Retrieve the value to write for the given {@link CassandraPersistentProperty} from
	 * {@link ConvertingPropertyAccessor} and perform optionally a conversion of collection element types.
	 *
	 * @param property the property.
	 * @param accessor the property accessor
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	private <T> T getWriteValue(CassandraPersistentProperty property, ConvertingPropertyAccessor accessor) {
		return (T) getWriteValue(accessor.getProperty(property, (Class<T>) getTargetType(property)),
				property.getTypeInformation());
	}

	/**
	 * Retrieve the value from {@code value} applying the given {@link TypeInformation} and perform optionally a
	 * conversion of collection element types.
	 *
	 * @param value the value, may be {@literal null}.
	 * @param typeInformation the type information.
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	private Object getWriteValue(@Nullable Object value, @Nullable TypeInformation<?> typeInformation) {

		if (value == null) {
			return null;
		}

		Class<?> requestedTargetType = typeInformation != null ? typeInformation.getType() : Object.class;

		if (getCustomConversions().hasCustomWriteTarget(value.getClass(), requestedTargetType)) {
			return getConversionService().convert(value, getCustomConversions()
					.getCustomWriteTarget(value.getClass(), requestedTargetType).orElse(requestedTargetType));
		}

		if (getCustomConversions().hasCustomWriteTarget(value.getClass())) {
			return getConversionService().convert(value, getCustomConversions().getCustomWriteTarget(value.getClass()).get());
		}

		if (getCustomConversions().isSimpleType(value.getClass())) {
			return getPotentiallyConvertedSimpleValue(value, requestedTargetType);
		}

		TypeInformation<?> type = typeInformation != null ? typeInformation
				: ClassTypeInformation.from((Class) value.getClass());

		TypeInformation<?> actualType = type.getRequiredActualType();

		if (value instanceof Collection) {

			Collection<Object> original = (Collection<Object>) value;
			Collection<Object> converted = CollectionFactory.createCollection(getCollectionType(type), original.size());

			for (Object element : original) {
				converted.add(convertToColumnType(element, actualType));
			}

			return converted;
		}

		BasicCassandraPersistentEntity<?> entity = getMappingContext().getPersistentEntity(actualType.getType());

		if (entity != null && entity.isUserDefinedType()) {

			UDTValue udtValue = entity.getUserType().newValue();

			write(value, udtValue, entity);

			return udtValue;
		}

		return value;
	}

	/**
	 * Performs special enum handling or simply returns the value as is.
	 *
	 * @param value may be {@literal null}.
	 * @param requestedTargetType must not be {@literal null}.
	 * @see CassandraType
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
			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return getConversionService().convert(value, target);
	}

	private static Class<?> getCollectionType(TypeInformation<?> type) {

		if (type.getType().isInterface()) {
			return type.getType();
		}

		if (ClassTypeInformation.LIST.isAssignableFrom(type)) {
			return ClassTypeInformation.LIST.getType();
		}

		if (ClassTypeInformation.SET.isAssignableFrom(type)) {
			return ClassTypeInformation.SET.getType();
		}

		if (!type.isCollectionLike()) {
			return ClassTypeInformation.LIST.getType();
		}

		return type.getType();
	}

	/**
	 * Retrieve the value to read for the given {@link CassandraPersistentProperty} from
	 * {@link BasicCassandraRowValueProvider} and perform optionally a conversion of collection element types.
	 *
	 * @param row the row.
	 * @param property the property.
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	protected Object getReadValue(CassandraValueProvider row, CassandraPersistentProperty property) {

		if (property.isCompositePrimaryKey()) {

			CassandraPersistentEntity<?> keyEntity = mappingContext.getRequiredPersistentEntity(property);
			return instantiatePrimaryKey(keyEntity, property, row);
		}

		Object value = row.getPropertyValue(property);

		if (value == null) {
			return null;
		}

		if (getCustomConversions().hasCustomWriteTarget(property.getActualType()) && property.isCollectionLike()) {

			if (value instanceof Collection) {

				Collection<Object> original = (Collection<Object>) value;

				Collection<Object> converted = CollectionFactory.createCollection(property.getType(), original.size());

				for (Object element : original) {
					converted.add(getConversionService().convert(element, property.getActualType()));
				}

				return converted;
			}
		}

		if (property.isCollectionLike() && value instanceof Collection) {
			return readCollectionOrArray(property.getTypeInformation(), (Collection<?>) value);
		}

		BasicCassandraPersistentEntity<?> persistentEntity = getMappingContext()
				.getPersistentEntity(property.getActualType());

		if (persistentEntity != null && persistentEntity.isUserDefinedType() && value instanceof UDTValue) {
			return readEntityFromUdt(persistentEntity, (UDTValue) value);
		}

		return getPotentiallyConvertedSimpleRead(value, property.getType());
	}

	/**
	 * Reads the given {@link Collection} into a collection of the given {@link TypeInformation}.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param sourceValue must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object readCollectionOrArray(TypeInformation<?> targetType, Collection<?> sourceValue) {

		Assert.notNull(targetType, "Target type must not be null!");

		Class<?> collectionType = targetType.getType();

		TypeInformation<?> componentType = targetType.getComponentType();
		Class<?> rawComponentType = componentType != null ? componentType.getType() : List.class;

		collectionType = Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;
		Collection<Object> items = targetType.getType().isArray() ? new ArrayList<>()
				: CollectionFactory.createCollection(collectionType, rawComponentType, sourceValue.size());

		if (sourceValue.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(items, collectionType);
		}

		BasicCassandraPersistentEntity<?> entity = getMappingContext().getPersistentEntity(rawComponentType);

		if (entity != null && entity.isUserDefinedType()) {

			for (Object udtValue : sourceValue) {
				items.add(readEntityFromUdt(entity, (UDTValue) udtValue));
			}

		} else {
			for (Object item : sourceValue) {
				items.add(getPotentiallyConvertedSimpleRead(item, rawComponentType));
			}
		}

		return getPotentiallyConvertedSimpleRead(items, targetType.getType());
	}

	private TypeCodec<Object> getCodec(CassandraPersistentProperty property) {
		return CodecRegistry.DEFAULT_INSTANCE.codecFor(mappingContext.getDataType(property));
	}

	/**
	 * {@link CassandraRowValueProvider} that delegates reads to {@link CassandraValueProvider} applying mapping and
	 * custom conversion from {@link MappingCassandraConverter}.
	 *
	 * @author Mark Paluch
	 * @since 1.5.1
	 */
	@AllArgsConstructor
	class MappingAndConvertingValueProvider implements CassandraValueProvider {

		private final CassandraValueProvider parent;

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.convert.CassandraValueProvider#hasProperty(org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty)
		 */
		@Override
		public boolean hasProperty(CassandraPersistentProperty property) {
			return parent.hasProperty(property);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
		 */
		@Nullable
		@Override
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(CassandraPersistentProperty property) {
			return (T) getReadValue(parent, property);
		}
	}
}
