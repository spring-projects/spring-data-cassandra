/*
 * Copyright 2013-2018 the original author or authors.
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
import java.util.function.Function;

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
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
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
		return this.mappingContext;
	}

	/**
	 * Read a {@link Row} into the requested target {@code type}.
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

		if (typeInfo.isCollectionLike() || typeInfo.isMap()) {
			return getConversionService().convert(row, type);
		}

		CassandraPersistentEntity<R> persistentEntity = (CassandraPersistentEntity<R>) getMappingContext()
				.getRequiredPersistentEntity(typeInfo);

		return readEntityFromRow(persistentEntity, row);
	}

	protected <S> S readEntityFromRow(CassandraPersistentEntity<S> entity, Row row) {
		return doRead(entity, row, expressionEvaluator -> new BasicCassandraRowValueProvider(row, expressionEvaluator));
	}

	protected <S> S readEntityFromUdt(CassandraPersistentEntity<S> entity, UDTValue udtValue) {
		return doRead(entity, udtValue,
				expressionEvaluator -> new CassandraUDTValueProvider(udtValue, getCodecRegistry(), expressionEvaluator));
	}

	protected <S> S readEntityFromTuple(CassandraPersistentEntity<S> entity, TupleValue tupleValue) {
		return doRead(entity, tupleValue,
				expressionEvaluator -> new CassandraTupleValueProvider(tupleValue, getCodecRegistry(), expressionEvaluator));
	}

	protected <S, V> S doRead(CassandraPersistentEntity<S> entity, V value,
			Function<SpELExpressionEvaluator, CassandraValueProvider> valueProviderSupplier) {

		DefaultSpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(value, spELContext);

		CassandraValueProvider valueProvider = valueProviderSupplier.apply(expressionEvaluator);

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

			CassandraPersistentEntity<?> keyEntity = getMappingContext().getRequiredPersistentEntity(property);

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

	protected Object instantiatePrimaryKey(CassandraPersistentEntity<?> entity, CassandraPersistentProperty keyProperty,
			CassandraValueProvider propertyProvider) {

		return this.instantiators.getInstantiatorFor(entity).createInstance(entity,
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
			return (Class<T>) ClassUtils.forName(entity.getName(), this.beanClassLoader);
		} catch (ClassNotFoundException | LinkageError ignore) {
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
			writeUDTValue(getConvertingAccessor(source, entity), (UDTValue) sink, entity);
		} else if (sink instanceof TupleValue) {
			writeTupleValue(getConvertingAccessor(source, entity), (TupleValue) sink, entity);
		} else {
			throw new MappingException("Unknown write target " + sink.getClass().getName());
		}
	}

	private void writeInsertFromObject(Object object, Insert insert, CassandraPersistentEntity<?> entity) {
		writeInsertFromWrapper(getConvertingAccessor(object, entity), insert, entity);
	}

	protected void writeInsertFromWrapper(ConvertingPropertyAccessor accessor, Insert insert,
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

				CassandraPersistentEntity<?> compositePrimaryKey = getMappingContext().getRequiredPersistentEntity(property);

				writeInsertFromWrapper(getConvertingAccessor(value, compositePrimaryKey), insert, compositePrimaryKey);

				continue;
			}

			if (value == null) {
				continue;
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding insert.value [{}] - [{}]", property.getRequiredColumnName().toCql(), value);
			}

			insert.value(property.getRequiredColumnName().toCql(), value);
		}
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

				CassandraPersistentEntity<?> compositePrimaryKey = getMappingContext().getRequiredPersistentEntity(property);

				writeMapFromWrapper(getConvertingAccessor(value, compositePrimaryKey), insert, compositePrimaryKey);

				continue;
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding map.entry [{}] - [{}]", property.getRequiredColumnName().toCql(), value);
			}

			insert.put(property.getRequiredColumnName().toCql(), value);
		}
	}

	protected void writeUpdateFromObject(Object object, Update update, CassandraPersistentEntity<?> entity) {
		writeUpdateFromWrapper(getConvertingAccessor(object, entity), update, entity);
	}

	protected void writeUpdateFromWrapper(ConvertingPropertyAccessor accessor, Update update,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			Object value = getWriteValue(property, accessor);

			if (property.isCompositePrimaryKey()) {

				CassandraPersistentEntity<?> compositePrimaryKey = getMappingContext().getRequiredPersistentEntity(property);

				if (value == null) {
					continue;
				}

				writeUpdateFromWrapper(getConvertingAccessor(value, compositePrimaryKey), update, compositePrimaryKey);

				continue;
			}

			if (isPrimaryKeyPart(property)) {
				update.where(QueryBuilder.eq(property.getRequiredColumnName().toCql(), value));
			} else {
				update.with(QueryBuilder.set(property.getRequiredColumnName().toCql(), value));
			}
		}
	}

	protected void writeSelectWhereFromObject(Object object, Select.Where where, CassandraPersistentEntity<?> entity) {
		getWhereClauses(object, entity).forEach(where::and);
	}

	protected void writeDeleteWhereFromObject(Object object, Delete.Where where, CassandraPersistentEntity<?> entity) {
		getWhereClauses(object, entity).forEach(where::and);
	}

	protected void writeUDTValue(ConvertingPropertyAccessor accessor, UDTValue udtValue,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			Object value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("writeUDTValueWhereFromObject Property.type {}, Property.value {}", property.getType().getName(),
						value);
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding udt.value [{}] - [{}]", property.getRequiredColumnName().toCql(), value);
			}

			TypeCodec<Object> typeCodec = getCodecRegistry().codecFor(getMappingContext().getDataType(property));

			udtValue.set(property.getRequiredColumnName().toCql(), value, typeCodec);
		}
	}

	protected void writeTupleValue(ConvertingPropertyAccessor accessor, TupleValue tupleValue,
			CassandraPersistentEntity<?> entity) {

		for (CassandraPersistentProperty property : entity) {

			Object value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("writeTupleValue Property.type {}, Property.value {}", property.getType().getName(), value);
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding tuple value [{}] - [{}]", property.getOrdinal(), value);
			}

			TypeCodec<Object> typeCodec = getCodecRegistry().codecFor(mappingContext.getDataType(property));

			tupleValue.set(property.getRequiredOrdinal(), value, typeCodec);
		}
	}

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
					? getMappingContext().getRequiredPersistentEntity(compositeIdProperty)
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

			CassandraPersistentEntity<?> compositePrimaryKey = getMappingContext()
					.getRequiredPersistentEntity(compositeIdProperty);

			return getWhereClauses(getConvertingAccessor(id, compositePrimaryKey), compositePrimaryKey);
		}

		Class<?> targetType = getTargetType(idProperty);

		return Collections.singleton(QueryBuilder.eq(idProperty.getRequiredColumnName().toCql(),
				getPotentiallyConvertedSimpleValue(id, targetType)));
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

	private Collection<Clause> getWhereClauses(ConvertingPropertyAccessor accessor, CassandraPersistentEntity<?> entity) {

		Assert.isTrue(entity.isCompositePrimaryKey(),
				String.format("Entity [%s] is not a composite primary key", entity.getName()));

		Collection<Clause> clauses = new ArrayList<>();

		for (CassandraPersistentProperty property : entity) {
			TypeCodec<Object> codec = getCodec(property);
			Object value = accessor.getProperty(property, codec.getJavaType().getRawType());
			clauses.add(QueryBuilder.eq(property.getRequiredColumnName().toCql(), value));
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

			clauses.add(QueryBuilder.eq(persistentProperty.getRequiredColumnName().toCql(), writeValue));
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

		PersistentPropertyAccessor propertyAccessor = source instanceof PersistentPropertyAccessor
				? (PersistentPropertyAccessor) source
				: entity.getPropertyAccessor(source);

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

		if (dataType instanceof UserType || dataType instanceof TupleType) {
			return property.getType();
		}

		TypeCodec<Object> codec = getCodecRegistry().codecFor(getMappingContext().getDataType(property));

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

		TypeInformation<?> type = typeInformation != null ? typeInformation
				: ClassTypeInformation.from((Class) value.getClass());

		if (value instanceof Collection) {
			return writeCollectionInternal((Collection<Object>) value, type);
		}

		if (value instanceof Map) {
			return writeMapInternal((Map<Object, Object>) value, type);
		}

		TypeInformation<?> actualType = type.getRequiredActualType();

		BasicCassandraPersistentEntity<?> entity = getMappingContext().getPersistentEntity(actualType.getType());

		if (entity != null) {

			if (entity.isUserDefinedType()) {

				UDTValue udtValue = entity.getUserType().newValue();

				write(value, udtValue, entity);

				return udtValue;
			}

			if (entity.isTupleType()) {

				TupleValue tupleValue = mappingContext.getTupleType(entity).newValue();

				write(value, tupleValue, entity);

				return tupleValue;
			}
		}

		return value;
	}

	private Object writeCollectionInternal(Collection<Object> source, TypeInformation<?> type) {

		Collection<Object> converted = CollectionFactory.createCollection(getCollectionType(type), source.size());

		TypeInformation<?> actualType = type.getRequiredActualType();

		for (Object element : source) {
			converted.add(convertToColumnType(element, actualType));
		}

		return converted;
	}

	private Object writeMapInternal(Map<Object, Object> source, TypeInformation<?> type) {

		Map<Object, Object> converted = CollectionFactory.createMap(type.getType(), source.size());

		TypeInformation<?> keyType = type.getRequiredComponentType();
		TypeInformation<?> valueType = type.getRequiredMapValueType();

		for (Entry<Object, Object> entry : source.entrySet()) {
			converted.put(convertToColumnType(entry.getKey(), keyType), convertToColumnType(entry.getValue(), valueType));
		}

		return converted;
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

			CassandraPersistentEntity<?> keyEntity = getMappingContext().getRequiredPersistentEntity(property);

			return instantiatePrimaryKey(keyEntity, property, row);
		}

		Object value = row.getPropertyValue(property);

		return value == null ? null : convertReadValue(value, property.getTypeInformation());
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private Object convertReadValue(Object value, TypeInformation<?> typeInformation) {

		if (getCustomConversions().hasCustomWriteTarget(typeInformation.getRequiredActualType().getType())
				&& typeInformation.isCollectionLike()) {

			if (value instanceof Collection) {

				Collection<Object> original = (Collection<Object>) value;

				Collection<Object> converted = CollectionFactory.createCollection(typeInformation.getType(), original.size());

				for (Object element : original) {
					converted.add(getConversionService().convert(element, typeInformation.getRequiredActualType().getType()));
				}

				return converted;
			}
		}

		if (typeInformation.isCollectionLike() && value instanceof Collection) {
			return readCollectionOrArrayInternal((Collection<?>) value, typeInformation);
		}

		if (typeInformation.isMap() && value instanceof Map) {
			return readMapInternal((Map<Object, Object>) value, typeInformation);
		}

		if (value instanceof UDTValue) {

			BasicCassandraPersistentEntity<?> udtEntity = getMappingContext()
					.getPersistentEntity(typeInformation.getRequiredActualType());

			if (udtEntity != null && udtEntity.isUserDefinedType()) {
				return readEntityFromUdt(udtEntity, (UDTValue) value);
			}
		}

		if (value instanceof TupleValue) {

			BasicCassandraPersistentEntity<?> tupleEntity = getMappingContext()
					.getPersistentEntity(typeInformation.getRequiredActualType());

			if (tupleEntity != null) {
				return readEntityFromTuple(tupleEntity, (TupleValue) value);
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
	@SuppressWarnings({ "rawtypes", "unchecked" })
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

		if (entity != null) {

			if (entity.isUserDefinedType()) {
				for (Object udtValue : source) {
					collection.add(readEntityFromUdt(entity, (UDTValue) udtValue));
				}
			} else if (entity.isTupleType()) {
				for (Object tupleValue : source) {
					collection.add(readEntityFromTuple(entity, (TupleValue) tupleValue));
				}
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
	@SuppressWarnings({ "rawtypes", "unchecked" })
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

	private TypeCodec<Object> getCodec(CassandraPersistentProperty property) {
		return getCodecRegistry().codecFor(mappingContext.getDataType(property));
	}

	private static CodecRegistry getCodecRegistry() {
		return CodecRegistry.DEFAULT_INSTANCE;
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
	}
}
