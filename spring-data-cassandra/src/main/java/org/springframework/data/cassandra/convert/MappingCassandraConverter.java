/*
 * Copyright 2013-2016 the original author or authors
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
package org.springframework.data.cassandra.convert;

import static org.springframework.data.cassandra.repository.support.BasicMapId.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.repository.MapIdentifiable;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
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
 * @see org.springframework.beans.factory.InitializingBean
 * @see org.springframework.context.ApplicationContextAware
 * @see org.springframework.beans.factory.BeanClassLoaderAware
 * @see org.springframework.data.convert.EntityConverter
 * @see org.springframework.data.convert.EntityReader
 * @see org.springframework.data.convert.EntityWriter
 */
public class MappingCassandraConverter extends AbstractCassandraConverter
	implements CassandraConverter, ApplicationContextAware, BeanClassLoaderAware {

	protected final CassandraMappingContext mappingContext;
	protected ApplicationContext applicationContext;
	protected ClassLoader beanClassLoader;
	protected SpELContext spELContext;

	private final Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * Creates a new {@link MappingCassandraConverter} with a {@link BasicCassandraMappingContext}.
	 */
	public MappingCassandraConverter() {
		this(new BasicCassandraMappingContext());
	}

	/**
	 * Creates a new {@link MappingCassandraConverter} with the given {@link CassandraMappingContext}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingCassandraConverter(CassandraMappingContext mappingContext) {

		super(new DefaultConversionService());

		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");

		this.mappingContext = mappingContext;
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
	}

	@SuppressWarnings("unchecked")
	public <R> R readRow(Class<R> type, Row row) {

		Class<R> beanClassLoaderClass = transformClassToBeanClassLoaderClass(type);
		TypeInformation<? extends R> typeInfo = ClassTypeInformation.from(beanClassLoaderClass);
		Class<? extends R> rawType = typeInfo.getType();

		if (Row.class.isAssignableFrom(rawType)) {
			return (R) row;
		}

		if (conversions.hasCustomReadTarget(Row.class, rawType) || conversionService.canConvert(Row.class, rawType)) {
			return conversionService.convert(row, rawType);
		}

		if (typeInfo.isCollectionLike() || typeInfo.isMap()) {
			return conversionService.convert(row, type);
		}

		CassandraPersistentEntity<R> persistentEntity =
			(CassandraPersistentEntity<R>) mappingContext.getPersistentEntity(typeInfo);

		if (persistentEntity == null) {
			throw new MappingException("No mapping metadata found for " + rawType.getName());
		}

		return readEntityFromRow(persistentEntity, row);
	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	protected <S> S readEntityFromRow(final CassandraPersistentEntity<S> entity, final Row row) {

		DefaultSpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(row, spELContext);
		BasicCassandraRowValueProvider rowValueProvider = new BasicCassandraRowValueProvider(row, expressionEvaluator);

		CassandraPersistentEntityParameterValueProvider parameterProvider =
			new CassandraPersistentEntityParameterValueProvider(entity, rowValueProvider, null);

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterProvider);

		readPropertiesFromRow(entity, rowValueProvider, getConvertingAccessor(instance, entity));

		return instance;
	}

	protected void readPropertiesFromRow(final CassandraPersistentEntity<?> entity,
		final BasicCassandraRowValueProvider row, final PersistentPropertyAccessor propertyAccessor) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty property) {
				MappingCassandraConverter.this.readPropertyFromRow(entity, property, row, propertyAccessor);
			}
		});
	}

	protected void readPropertyFromRow(CassandraPersistentEntity<?> entity, CassandraPersistentProperty property,
			BasicCassandraRowValueProvider row, PersistentPropertyAccessor propertyAccessor) {

		// if true then skip; property was set in constructor
		if (entity.isConstructorArgument(property)) {
			return;
		}

		if (property.isCompositePrimaryKey()) {

			CassandraPersistentProperty keyProperty = entity.getIdProperty();
			CassandraPersistentEntity<?> keyEntity = keyProperty.getCompositePrimaryKeyEntity();

			Object key = propertyAccessor.getProperty(keyProperty);

			if (key == null) {
				key = instantiatePrimaryKey(keyEntity, keyProperty, row);
			}

			// now recurse on using the key this time
			readPropertiesFromRow(property.getCompositePrimaryKeyEntity(), row, getConvertingAccessor(key, keyEntity));

			// now that the key's properties have been populated, set the key property on the entity
			propertyAccessor.setProperty(keyProperty, key);

			return;
		}

		if (!row.getRow().getColumnDefinitions().contains(property.getColumnName().toCql())) {
			return;
		}

		Object obj = getReadValue(property, row);

		propertyAccessor.setProperty(property, obj);
	}

	@SuppressWarnings("unused")
	protected Object instantiatePrimaryKey(CassandraPersistentEntity<?> entity, CassandraPersistentProperty keyProperty,
			BasicCassandraRowValueProvider propertyProvider) {

		return instantiators.getInstantiatorFor(entity).createInstance(entity,
				new CassandraPersistentEntityParameterValueProvider(entity, propertyProvider, null));
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

	@Override
	public void write(Object source, Object sink) {

		if (source != null) {
			Class<?> beanClassLoaderClass = transformClassToBeanClassLoaderClass(source.getClass());
			CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(beanClassLoaderClass);

			write(source, sink, entity);
		}
	}

	@Override
	public void write(Object source, Object sink, CassandraPersistentEntity<?> entity) {

		if (source == null) {
			return;
		}

		if (entity == null) {
			throw new MappingException("No mapping metadata found for " + source.getClass());
		}

		if (sink instanceof Insert) {
			writeInsertFromObject(source, (Insert) sink, entity);
		} else if (sink instanceof Update) {
			writeUpdateFromObject(source, (Update) sink, entity);
		} else if (sink instanceof Select.Where) {
			writeSelectWhereFromObject(source, (Select.Where) sink, entity);
		} else if (sink instanceof Delete.Where) {
			writeDeleteWhereFromObject(source, (Delete.Where) sink, entity);
		} else {
			throw new MappingException("Unknown write target " + sink.getClass().getName());
		}
	}

	protected void writeInsertFromObject(final Object object, final Insert insert, CassandraPersistentEntity<?> entity) {
		writeInsertFromWrapper(getConvertingAccessor(object, entity), insert, entity);
	}

	protected void writeInsertFromWrapper(final ConvertingPropertyAccessor accessor, final Insert insert,
			CassandraPersistentEntity<?> entity) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty property) {

				Object value = getWriteValue(property, accessor);

				if (log.isDebugEnabled()) {
					log.debug("doWithProperties Property.type {}, Property.value {}", property.getType().getName(), value);
				}

				if (property.isCompositePrimaryKey()) {
					if (log.isDebugEnabled()) {
						log.debug("Property is a compositeKey");
					}

					writeInsertFromWrapper(getConvertingAccessor(value, property.getCompositePrimaryKeyEntity()), insert,
							property.getCompositePrimaryKeyEntity());

					return;
				}

				if (log.isDebugEnabled()) {
					log.debug("Adding insert.value [{}] - [{}]", property.getColumnName().toCql(), value);
				}

				insert.value(property.getColumnName().toCql(), value);
			}
		});
	}

	protected void writeUpdateFromObject(final Object object, final Update update, CassandraPersistentEntity<?> entity) {
		writeUpdateFromWrapper(getConvertingAccessor(object, entity), update, entity);
	}

	protected void writeUpdateFromWrapper(final ConvertingPropertyAccessor accessor, final Update update,
		final CassandraPersistentEntity<?> entity) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty property) {

				Object value = getWriteValue(property, accessor);

				if (property.isCompositePrimaryKey()) {
					CassandraPersistentEntity<?> keyEntity = property.getCompositePrimaryKeyEntity();
					writeUpdateFromWrapper(getConvertingAccessor(value, keyEntity), update, keyEntity);
					return;
				}

				if (isPrimaryKeyPart(property)) {
					update.where(QueryBuilder.eq(property.getColumnName().toCql(), value));
				} else {
					update.with(QueryBuilder.set(property.getColumnName().toCql(), value));
				}
			}
		});
	}

	protected void writeSelectWhereFromObject(final Object object, final Select.Where where,
			CassandraPersistentEntity<?> entity) {

		Collection<Clause> clauses = getWhereClauses(object, entity);

		for (Clause clause : clauses) {
			where.and(clause);
		}
	}

	protected void writeDeleteWhereFromObject(final Object object, final Delete.Where where,
			CassandraPersistentEntity<?> entity) {

		Collection<Clause> clauses = getWhereClauses(object, entity);

		for (Clause clause : clauses) {
			where.and(clause);
		}
	}

	private Collection<Clause> getWhereClauses(Object source, CassandraPersistentEntity<?> entity) {

		Assert.notNull(source, "Id source must not be null");

		CassandraPersistentProperty idProperty = entity.getIdProperty();

		Object id = extractId(source, entity);

		if (id == null) {
			String message = String.format("No Id value found in object %s", source);
			throw new IllegalArgumentException(message);
		}

		if (id instanceof MapId) {
			return getWhereClauses((MapId) id, idProperty != null && idProperty.isCompositePrimaryKey() ? idProperty.getCompositePrimaryKeyEntity() : entity);
		}

		if (idProperty == null) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Cannot obtain where clauses for entity [%s] using [%s]", entity.getName(), source));
		}

		if (idProperty.isCompositePrimaryKey()) {

			if (ClassUtils.isAssignableValue(idProperty.getType(), id)) {
				return getWhereClauses(getConvertingAccessor(id, idProperty.getCompositePrimaryKeyEntity()),
						idProperty.getCompositePrimaryKeyEntity());
			} else {
				throw new InvalidDataAccessApiUsageException(
						String.format("Cannot use [%s] as composite Id for [%s]", id, entity.getName()));
			}
		}

		TypeCodec<Object> codec = getCodec(idProperty);

		if(conversionService.canConvert(id.getClass(), codec.getJavaType().getRawType())){
			return Collections.singleton(QueryBuilder.eq(idProperty.getColumnName().toCql(), conversionService.convert(id, codec.getJavaType().getRawType())));
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

		final Collection<Clause> clauses = new ArrayList<Clause>();
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty property) {

				TypeCodec<Object> codec = getCodec(property);
				Object value = accessor.getProperty(property,
					codec.getJavaType().getRawType());
				clauses.add(QueryBuilder.eq(property.getColumnName().toCql(), value));
			}
		});

		return clauses;
	}

	private Collection<Clause> getWhereClauses(MapId id, CassandraPersistentEntity<?> entity) {

		Assert.notNull(id, "MapId must not be null");

		Collection<Clause> clauses = new ArrayList<Clause>();

		for (Entry<String, Serializable> entry : id.entrySet()) {
			CassandraPersistentProperty persistentProperty = entity.getPersistentProperty(entry.getKey());
			if (persistentProperty == null) {
				throw new IllegalArgumentException(String.format("MapId contains references [%s] that is an unknown property of [%s]", entry.getKey(), entity.getName()));
			}

			clauses.add(QueryBuilder.eq(persistentProperty.getColumnName().toCql(), getWriteValue(persistentProperty, entry.getValue())));
		}

		return clauses;
	}

	@Override
	public Object getId(Object object, CassandraPersistentEntity<?> entity) {

		Assert.notNull(object, "Object instance must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		final ConvertingPropertyAccessor accessor = getConvertingAccessor(object, entity);

		if (!entity.getType().isAssignableFrom(object.getClass())) {
			throw new IllegalArgumentException(
					String.format("Given instance of type [%s] is not of compatible expected type [%s]",
							object.getClass().getName(), entity.getType().getName()));
		}

		if (object instanceof MapIdentifiable) {
			return ((MapIdentifiable) object).getMapId();
		}

		CassandraPersistentProperty idProperty = entity.getIdProperty();

		if (idProperty != null) {
			return accessor.getProperty(idProperty, (Class<?>) (idProperty.isCompositePrimaryKey() ? idProperty.getType()
					: getCodec(idProperty).getJavaType().getRawType()));
		}

		// if the class doesn't have an id property, then it's using MapId
		final MapId id = id();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty property) {
				if (property.isPrimaryKeyColumn()) {
					id.with(property.getName(), (Serializable) getWriteValue(property, accessor));
				}
			}
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
	 * Creates a new {@link ConvertingPropertyAccessor} for the given source and entity.
	 *
	 * @param source must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return a new {@link ConvertingPropertyAccessor} for the given source and entity.
	 */
	private ConvertingPropertyAccessor getConvertingAccessor(Object source, CassandraPersistentEntity<?> entity) {

		PersistentPropertyAccessor propertyAccessor = (source instanceof PersistentPropertyAccessor
			? (PersistentPropertyAccessor) source : entity.getPropertyAccessor(source));

		return new ConvertingPropertyAccessor(propertyAccessor, conversionService);
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
		return (property.isCompositePrimaryKey() ? property.getType() : getCodec(property).getJavaType().getRawType());
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
	private Object getWriteValue(CassandraPersistentProperty property, ConvertingPropertyAccessor accessor) {
		return getWriteValue(property, accessor.getProperty(property, getTargetType(property)));
	}

	/**
	 * Retrieve the value to write for the given {@link CassandraPersistentProperty} from
	 * {@link ConvertingPropertyAccessor} and perform optionally a conversion of collection element types.
	 *
	 * @param property the property.
	 * @param value the value
	 * @return the return value, may be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	private Object getWriteValue(CassandraPersistentProperty property, Object value) {

		if (value != null) {

			if (conversions.hasCustomWriteTarget(property.getActualType()) && property.isCollectionLike()) {
				Class<?> customWriteTarget = conversions.getCustomWriteTarget(property.getActualType());

				if (Collection.class.isAssignableFrom(property.getType()) && value instanceof Collection) {

					Collection<Object> original = (Collection<Object>) value;
					Collection<Object> converted = CollectionFactory.createCollection(property.getType(), original.size());

					for (Object o : original) {
						converted.add(getConversionService().convert(o, customWriteTarget));
					}

					value = converted;
				}
			}
		}

		return value;
	}

	/**
	 * Retrieve the value to read for the given {@link CassandraPersistentProperty} from
	 * {@link BasicCassandraRowValueProvider} and perform optionally a conversion of collection element types.
	 *
	 * @param property the property.
	 * @param row the row.
	 * @return the return value, may be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	private Object getReadValue(CassandraPersistentProperty property, BasicCassandraRowValueProvider row) {

		Object obj = row.getPropertyValue(property);

		if (obj != null) {

			if (conversions.hasCustomWriteTarget(property.getActualType()) && property.isCollectionLike()) {

				if (Collection.class.isAssignableFrom(property.getType()) && obj instanceof Collection) {

					Collection<Object> original = (Collection<Object>) obj;

					Collection<Object> converted = CollectionFactory.createCollection(
						property.getType(), original.size());

					for (Object element : original) {
						converted.add(getConversionService().convert(element, property.getActualType()));
					}

					return converted;
				}
			}
		}

		return obj;
	}

	private TypeCodec<Object> getCodec(CassandraPersistentProperty property) {
		return CodecRegistry.DEFAULT_INSTANCE.codecFor(mappingContext.getDataType(property));
	}
}
