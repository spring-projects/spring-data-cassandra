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

import static org.springframework.data.cassandra.repository.support.BasicMapId.*;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.repository.MapIdentifiable;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
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

	private ClassLoader beanClassLoader;

	private SpELContext spELContext;

	/**
	 * Create a new {@link MappingCassandraConverter} with a {@link CassandraMappingContext}.
	 */
	public MappingCassandraConverter() {
		this(new CassandraMappingContext());
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

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

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
				entity, new MappingAndConvertingValueProvider(rowValueProvider), Optional.empty());

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterValueProvider);

		readPropertiesFromRow(entity, rowValueProvider, getConvertingAccessor(instance, entity));

		return instance;
	}

	protected <S> S readEntityFromUdt(CassandraPersistentEntity<S> entity, UDTValue udtValue) {

		DefaultSpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(udtValue, spELContext);

		CassandraUDTValueProvider valueProvider = new CassandraUDTValueProvider(udtValue, CodecRegistry.DEFAULT_INSTANCE,
				expressionEvaluator);

		PersistentEntityParameterValueProvider<CassandraPersistentProperty> parameterValueProvider = new PersistentEntityParameterValueProvider<>(
				entity, new MappingAndConvertingValueProvider(valueProvider), Optional.empty());

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterValueProvider);

		readProperties(entity, valueProvider, getConvertingAccessor(instance, entity));

		return instance;
	}

	protected void readPropertiesFromRow(CassandraPersistentEntity<?> entity, CassandraRowValueProvider row,
			PersistentPropertyAccessor propertyAccessor) {

		readProperties(entity, row, propertyAccessor);
	}

	protected void readProperties(CassandraPersistentEntity<?> entity, CassandraValueProvider valueProvider,
			PersistentPropertyAccessor propertyAccessor) {

		entity.getPersistentProperties().forEach(
				property -> MappingCassandraConverter.this.readProperty(entity, property, valueProvider, propertyAccessor));
	}

	protected void readProperty(CassandraPersistentEntity<?> entity, CassandraPersistentProperty property,
			CassandraValueProvider valueProvider, PersistentPropertyAccessor propertyAccessor) {

		// if true then skip; property was set in constructor
		if (entity.isConstructorArgument(property)) {
			return;
		}

		if (property.isCompositePrimaryKey()) {

			CassandraPersistentEntity<?> keyEntity = property.getCompositePrimaryKeyEntity();

			Optional<Object> optionalKey = propertyAccessor.getProperty(property);

			if (!optionalKey.isPresent()) {
				optionalKey = Optional.of(instantiatePrimaryKey(keyEntity, property, valueProvider));
			}

			// now recurse on using the key this time
			optionalKey.ifPresent(key -> readProperties(property.getCompositePrimaryKeyEntity(), valueProvider,
					getConvertingAccessor(key, keyEntity)));

			// now that the key's properties have been populated, set the key property on the entity
			propertyAccessor.setProperty(property, optionalKey);

			return;
		}

		if (!valueProvider.hasProperty(property)) {
			return;
		}

		propertyAccessor.setProperty(property, getReadValue(valueProvider, property));
	}

	@SuppressWarnings("unused")
	protected Object instantiatePrimaryKey(CassandraPersistentEntity<?> entity, CassandraPersistentProperty keyProperty,
			PropertyValueProvider<CassandraPersistentProperty> propertyProvider) {

		return instantiators.getInstantiatorFor(entity).createInstance(entity,
				new PersistentEntityParameterValueProvider<>(entity, propertyProvider, Optional.empty()));
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
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#convertToColumnType(java.util.Optional)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Optional<Object> convertToColumnType(Optional<T> obj) {

		return convertToColumnType(obj, obj.map(Object::getClass).map(ClassTypeInformation::from)
				.orElse((ClassTypeInformation) ClassTypeInformation.OBJECT));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#convertToColumnType(java.util.Optional, org.springframework.data.util.TypeInformation)
	 */
	@Override
	public <T> Optional<Object> convertToColumnType(Optional<T> obj, TypeInformation<?> typeInformation) {

		Assert.notNull(typeInformation, "TypeInformation must not be null");

		return obj.flatMap(object -> {

			if (object.getClass().isArray()) {
				return Optional.of(object);
			}

			return getWriteValue(obj, typeInformation);
		});
	}

	@Override
	public void write(Object source, Object sink) {

		if (source != null) {
			Class<?> beanClassLoaderClass = transformClassToBeanClassLoaderClass(source.getClass());
			CassandraPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(beanClassLoaderClass);

			write(source, sink, entity);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void write(Object source, Object sink, CassandraPersistentEntity<?> entity) {

		if (source == null) {
			return;
		}

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

	protected void writeInsertFromObject(final Object object, final Insert insert, CassandraPersistentEntity<?> entity) {
		writeInsertFromWrapper(getConvertingAccessor(object, entity), insert, entity);
	}

	private void writeMapFromWrapper(final ConvertingPropertyAccessor accessor, final Map<String, Object> insert,
			CassandraPersistentEntity<?> entity) {

		entity.getPersistentProperties().forEach(property -> {

			Optional<Object> value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("doWithProperties Property.type {}, Property.value {}", property.getType().getName(), value);
			}

			if (property.isCompositePrimaryKey()) {
				if (log.isDebugEnabled()) {
					log.debug("Property is a compositeKey");
				}

				writeMapFromWrapper(getConvertingAccessor(value.orElse(null), property.getCompositePrimaryKeyEntity()), insert,
						property.getCompositePrimaryKeyEntity());

				return;
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding map.entry [{}] - [{}]", property.getColumnName().toCql(), value);
			}

			insert.put(property.getColumnName().toCql(), value.orElse(null));
		});
	}

	protected void writeInsertFromWrapper(final ConvertingPropertyAccessor accessor, final Insert insert,
			CassandraPersistentEntity<?> entity) {

		entity.getPersistentProperties().forEach(property -> {

			Optional<Object> value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("doWithProperties Property.type {}, Property.value {}", property.getType().getName(), value);
			}

			if (property.isCompositePrimaryKey()) {
				if (log.isDebugEnabled()) {
					log.debug("Property is a compositeKey");
				}

				writeInsertFromWrapper(getConvertingAccessor(value.orElse(null), property.getCompositePrimaryKeyEntity()),
						insert, property.getCompositePrimaryKeyEntity());

				return;
			}

			if (!value.isPresent()) {
				return;
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding insert.value [{}] - [{}]", property.getColumnName().toCql(), value);
			}

			insert.value(property.getColumnName().toCql(), value.orElse(null));
		});
	}

	protected void writeUpdateFromObject(final Object object, final Update update, CassandraPersistentEntity<?> entity) {
		writeUpdateFromWrapper(getConvertingAccessor(object, entity), update, entity);
	}

	protected void writeUpdateFromWrapper(final ConvertingPropertyAccessor accessor, final Update update,
			final CassandraPersistentEntity<?> entity) {

		entity.getPersistentProperties().forEach(property -> {

			Optional<Object> value = getWriteValue(property, accessor);

			if (property.isCompositePrimaryKey()) {
				CassandraPersistentEntity<?> keyEntity = property.getCompositePrimaryKeyEntity();
				writeUpdateFromWrapper(getConvertingAccessor(value.orElse(null), keyEntity), update, keyEntity);
				return;
			}

			if (isPrimaryKeyPart(property)) {
				update.where(QueryBuilder.eq(property.getColumnName().toCql(), value.orElse(null)));
			} else {
				update.with(QueryBuilder.set(property.getColumnName().toCql(), value.orElse(null)));
			}
		});
	}

	protected void writeSelectWhereFromObject(final Object object, final Select.Where where,
			CassandraPersistentEntity<?> entity) {
		getWhereClauses(object, entity).forEach(where::and);
	}

	protected void writeDeleteWhereFromObject(final Object object, final Delete.Where where,
			CassandraPersistentEntity<?> entity) {
		getWhereClauses(object, entity).forEach(where::and);
	}

	protected void writeUDTValueWhereFromObject(final ConvertingPropertyAccessor accessor, final UDTValue udtValue,
			CassandraPersistentEntity<?> entity) {

		entity.getPersistentProperties().forEach(property -> {

			Optional<Object> value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug("writeUDTValueWhereFromObject Property.type {}, Property.value {}", property.getType().getName(),
						value);
			}

			if (log.isDebugEnabled()) {
				log.debug("Adding udt.value [{}] - [{}]", property.getColumnName().toCql(), value);
			}

			TypeCodec<Object> typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(getMappingContext().getDataType(property));

			udtValue.set(property.getColumnName().toCql(), value.orElse(null), typeCodec);
		});
	}

	private Collection<Clause> getWhereClauses(Object source, CassandraPersistentEntity<?> entity) {

		Assert.notNull(source, "Id source must not be null");

		Object id = extractId(source, entity);
		Assert.notNull(id, String.format("No Id value found in object %s", source));

		Optional<CassandraPersistentProperty> optionalIdProperty = entity.getIdProperty();

		Optional<CassandraPersistentProperty> optionalCompositeIdProperty = optionalIdProperty
				.filter(CassandraPersistentProperty::isCompositePrimaryKey);

		if (id instanceof MapId) {

			// FIXME: Generics
			CassandraPersistentEntity<?> whereEntity = optionalCompositeIdProperty //
					.map(CassandraPersistentProperty::getCompositePrimaryKeyEntity) //
					.orElse((CassandraPersistentEntity) entity);

			return getWhereClauses((MapId) id, whereEntity);
		}

		CassandraPersistentProperty idProperty = optionalIdProperty
				.orElseThrow(() -> new InvalidDataAccessApiUsageException(
						String.format("Cannot obtain where clauses for entity [%s] using [%s]", entity.getName(), source)));

		if (optionalCompositeIdProperty.isPresent()) {

			CassandraPersistentProperty compositeIdProperty = optionalCompositeIdProperty
					.filter(p -> ClassUtils.isAssignableValue(p.getType(), id))
					.orElseThrow(() -> new InvalidDataAccessApiUsageException(
							String.format("Cannot use [%s] as composite Id for [%s]", id, entity.getName())));

			return getWhereClauses(getConvertingAccessor(id, compositeIdProperty.getCompositePrimaryKeyEntity()),
					compositeIdProperty.getCompositePrimaryKeyEntity());
		}

		Class<?> targetType = getTargetType(idProperty);

		if (getConversionService().canConvert(id.getClass(), targetType)) {
			return Collections.singleton(QueryBuilder.eq(idProperty.getColumnName().toCql(),
					getPotentiallyConvertedSimpleValue(Optional.of(id), (Class<Object>) targetType).orElse(null)));
		}

		return Collections.singleton(QueryBuilder.eq(idProperty.getColumnName().toCql(), id));
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

		entity.getPersistentProperties().forEach(property -> {
			TypeCodec<Object> codec = getCodec(property);
			Optional<Object> value = accessor.getProperty(property, codec.getJavaType().getRawType());
			clauses.add(QueryBuilder.eq(property.getColumnName().toCql(), value.orElse(null)));
		});

		return clauses;
	}

	private Collection<Clause> getWhereClauses(MapId id, CassandraPersistentEntity<?> entity) {

		Assert.notNull(id, "MapId must not be null");

		Collection<Clause> clauses = new ArrayList<>();

		for (Entry<String, Object> entry : id.entrySet()) {

			Optional<CassandraPersistentProperty> lookup = entity.getPersistentProperty(entry.getKey());

			CassandraPersistentProperty persistentProperty = lookup
					.orElseThrow(() -> new IllegalArgumentException(String.format(
							"MapId contains references [%s] that is an unknown property of [%s]", entry.getKey(), entity.getName())));

			Optional<Object> writeValue = getWriteValue(Optional.ofNullable(entry.getValue()),
					persistentProperty.getTypeInformation());
			clauses.add(QueryBuilder.eq(persistentProperty.getColumnName().toCql(), writeValue.orElse(null)));
		}

		return clauses;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object getId(Object object, CassandraPersistentEntity<?> entity) {

		Assert.notNull(object, "Object instance must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		final ConvertingPropertyAccessor accessor = getConvertingAccessor(object, entity);

		Assert.isTrue(entity.getType().isAssignableFrom(object.getClass()),
				String.format("Given instance of type [%s] is not of compatible expected type [%s]",
						object.getClass().getName(), entity.getType().getName()));

		if (object instanceof MapIdentifiable) {
			return ((MapIdentifiable) object).getMapId();
		}

		Optional<CassandraPersistentProperty> optionalIdProperty = entity.getIdProperty();

		if (optionalIdProperty.isPresent()) {
			// TODO: NullId
			CassandraPersistentProperty idProperty = optionalIdProperty.get();
			return accessor.getProperty(idProperty, idProperty.isCompositePrimaryKey() ? (Class<Object>) idProperty.getType()
					: (Class<Object>) getTargetType(idProperty)).orElse(null);
		}

		// if the class doesn't have an id property, then it's using MapId
		final MapId id = id();

		entity.getPersistentProperties() //
				.filter(CassandraPersistentProperty::isPrimaryKeyColumn) //
				.forEach(property -> {
					id.with(property.getName(), getWriteValue(property, accessor).orElse(null));
				});

		return id;
	}

	@SuppressWarnings("unchecked")
	protected <T> Class<T> transformClassToBeanClassLoaderClass(Class<T> entity) {
		try {
			return (Class<T>) ClassUtils.forName(entity.getName(), beanClassLoader);
		} catch (ClassNotFoundException e) {
			return entity;
		} catch (LinkageError e) {
			return entity;
		}
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	@Override
	public CassandraMappingContext getMappingContext() {
		return mappingContext;
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
				? (PersistentPropertyAccessor) source : entity.getPropertyAccessor(source));

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

			if (property.findAnnotation(CassandraType.class).isPresent()) {
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
	@SuppressWarnings("unchecked")
	private <T> Optional<T> getWriteValue(CassandraPersistentProperty property, ConvertingPropertyAccessor accessor) {
		return getWriteValue(accessor.getProperty(property, (Class<T>) getTargetType(property)),
				property.getTypeInformation());
	}

	/**
	 * Retrieve the value from {@code value} applying the given {@link TypeInformation} and perform optionally a
	 * conversion of collection element types.
	 *
	 * @param optional the value, may be {@literal null}.
	 * @param typeInformation the type information.
	 * @return the return value, may be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	private <I, O> Optional<O> getWriteValue(Optional<I> optional, TypeInformation<?> typeInformation) {

		if (!optional.isPresent()) {
			return Optional.empty();
		}

		I value = optional.get();

		Class<O> requestedTargetType = Optional.ofNullable(typeInformation).map(typeInfo -> (Class<O>) typeInfo.getType())
				.orElse(null);

		if (getCustomConversions().hasCustomWriteTarget(value.getClass(), requestedTargetType)) {
			return Optional.ofNullable((O) getConversionService().convert(value, getCustomConversions()
					.getCustomWriteTarget(value.getClass(), requestedTargetType).orElse(requestedTargetType)));
		}

		if (getCustomConversions().hasCustomWriteTarget(value.getClass())) {
			return Optional.ofNullable((O) getConversionService().convert(value,
					getCustomConversions().getCustomWriteTarget(value.getClass()).get()));
		}

		if (getCustomConversions().isSimpleType(value.getClass())) {
			return getPotentiallyConvertedSimpleValue(optional, requestedTargetType);
		}

		TypeInformation<?> type = Optional.ofNullable(typeInformation)
				.orElseGet(() -> ClassTypeInformation.from((Class) value.getClass()));

		TypeInformation<?> actualType = type.getActualType();

		if (value instanceof Collection) {

			Collection<Object> original = (Collection<Object>) value;
			Collection<Object> converted = CollectionFactory.createCollection(getCollectionType(type), original.size());

			original.stream().map(element -> convertToColumnType(Optional.ofNullable(element), actualType).orElse(null))
					.forEach(converted::add);

			return Optional.of((O) converted);
		}

		Optional<BasicCassandraPersistentEntity<?>> optionalUdt = getMappingContext()
				.getPersistentEntity(actualType.getType()).filter(CassandraPersistentEntity::isUserDefinedType);

		if (optionalUdt.isPresent()) {

			return optionalUdt.map(persistentEntity -> {

				UDTValue udtValue = persistentEntity.getUserType().newValue();

				write(value, udtValue, persistentEntity);

				return (O) udtValue;
			});

		}

		return (Optional<O>) optional;
	}

	/**
	 * Performs special enum handling or simply returns the value as is.
	 *
	 * @param optionalValue may be {@literal null}.
	 * @param requestedTargetType must not be {@literal null}.
	 * @see CassandraType
	 */
	@SuppressWarnings("unchecked")
	private <I, O> Optional<O> getPotentiallyConvertedSimpleValue(Optional<I> optionalValue,
			Class<O> requestedTargetType) {

		if (optionalValue.isPresent()) {

			Object value = optionalValue.get();

			// Cassandra has no default enum handling - convert it to either a String
			// or, if requested, to a different type
			if (Enum.class.isAssignableFrom(value.getClass())) {
				if (requestedTargetType != null && !requestedTargetType.isEnum()
						&& getConversionService().canConvert(value.getClass(), requestedTargetType)) {

					return Optional.ofNullable(getConversionService().convert(value, requestedTargetType));
				}

				return Optional.of((O) ((Enum<?>) value).name());
			}
		}

		return (Optional<O>) optionalValue;
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
	private Object getPotentiallyConvertedSimpleRead(Object value, Class<?> target) {

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

	private Class<?> getCollectionType(TypeInformation<?> type) {

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
	@SuppressWarnings("unchecked")
	private <T> Optional<T> getReadValue(PropertyValueProvider<CassandraPersistentProperty> row,
			CassandraPersistentProperty property) {

		Optional<Object> obj = row.getPropertyValue(property);

		if (!obj.isPresent()) {
			return Optional.empty();
		}

		if (getCustomConversions().hasCustomWriteTarget(property.getActualType()) && property.isCollectionLike()) {

			if (obj.filter(it -> it instanceof Collection).isPresent()) {

				return obj.map(it -> {

					Collection<Object> original = (Collection<Object>) it;

					Collection<Object> converted = CollectionFactory.createCollection(property.getType(), original.size());

					for (Object element : original) {
						converted.add(getConversionService().convert(element, property.getActualType()));
					}

					return (T) converted;
				});

			}
		}

		if (property.isCollectionLike() && obj.filter(it -> it instanceof Collection).isPresent()) {
			return obj.map(it -> (T) readCollectionOrArray(property.getTypeInformation(), (Collection) it));
		}

		Optional<BasicCassandraPersistentEntity<?>> persistentEntity = getMappingContext()
				.getPersistentEntity(property.getActualType()).filter(CassandraPersistentEntity::isUserDefinedType);

		if (persistentEntity.isPresent() && obj.filter(it -> it instanceof UDTValue).isPresent()) {
			return persistentEntity.flatMap(
					cassandraPersistentEntity -> obj.map(it -> (T) readEntityFromUdt(cassandraPersistentEntity, (UDTValue) it)));
		}

		return obj.flatMap(it -> Optional.of((T) getPotentiallyConvertedSimpleRead(it, property.getType())));
	}

	/**
	 * Reads the given {@link Collection} into a collection of the given {@link TypeInformation}.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param sourceValue must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object readCollectionOrArray(TypeInformation<?> targetType, Collection<?> sourceValue) {

		Assert.notNull(targetType, "Target type must not be null!");

		Class<?> collectionType = targetType.getType();

		Optional<TypeInformation<?>> componentType = targetType.getComponentType();
		Class<?> rawComponentType = componentType.map(TypeInformation::getType).orElse((Class) List.class);

		collectionType = Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;
		Collection<Object> items = targetType.getType().isArray() ? new ArrayList<Object>()
				: CollectionFactory.createCollection(collectionType, rawComponentType, sourceValue.size());

		if (sourceValue.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(items, collectionType);
		}

		Optional<BasicCassandraPersistentEntity<?>> cassandraPersistentEntity = componentType
				.flatMap(it -> getMappingContext().getPersistentEntity(it))
				.filter(CassandraPersistentEntity::isUserDefinedType);

		if (cassandraPersistentEntity.isPresent()) {

			cassandraPersistentEntity.ifPresent(persistentEntity -> {
				for (Object udtValue : sourceValue) {
					items.add(readEntityFromUdt(persistentEntity, (UDTValue) udtValue));
				}
			});

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
		 * @see org.springframework.data.cassandra.core.convert.CassandraValueProvider#hasProperty(org.springframework.data.cassandra.mapping.CassandraPersistentProperty)
		 */
		@Override
		public boolean hasProperty(CassandraPersistentProperty property) {
			return parent.hasProperty(property);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
		 */
		@Override
		public <T> Optional<T> getPropertyValue(CassandraPersistentProperty property) {
			return getReadValue(parent, property);
		}
	}
}
