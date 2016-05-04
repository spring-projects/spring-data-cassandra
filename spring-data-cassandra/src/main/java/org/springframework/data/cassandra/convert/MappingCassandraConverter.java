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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.support.DefaultConversionService;
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

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
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
 * @see org.springframework.beans.factory.InitializingBean
 * @see org.springframework.context.ApplicationContextAware
 * @see org.springframework.beans.factory.BeanClassLoaderAware
 * @see org.springframework.data.convert.EntityConverter
 * @see org.springframework.data.convert.EntityReader
 * @see org.springframework.data.convert.EntityWriter
 */
public class MappingCassandraConverter extends AbstractCassandraConverter
		implements CassandraConverter, ApplicationContextAware, BeanClassLoaderAware {

	protected final Logger log = LoggerFactory.getLogger(getClass());
	protected final CassandraMappingContext mappingContext;

	protected ApplicationContext applicationContext;
	protected SpELContext spELContext;
	protected ClassLoader beanClassLoader;

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

		Assert.notNull(mappingContext);

		this.mappingContext = mappingContext;
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
	}

	@SuppressWarnings("unchecked")
	public <R> R readRow(Class<R> clazz, Row row) {

		Class<R> beanClassLoaderClass = transformClassToBeanClassLoaderClass(clazz);

		TypeInformation<? extends R> type = ClassTypeInformation.from(beanClassLoaderClass);
		TypeInformation<? extends R> typeToUse = type;
		Class<? extends R> rawType = typeToUse.getType();

		if (Row.class.isAssignableFrom(rawType)) {
			return (R) row;
		}

		if (conversions.hasCustomReadTarget(Row.class, rawType) || conversionService.canConvert(Row.class, rawType)) {
			return conversionService.convert(row, rawType);
		}

		if (type.isCollectionLike() || type.isMap()) {
			return conversionService.convert(row, clazz);
		}

		CassandraPersistentEntity<R> persistentEntity = (CassandraPersistentEntity<R>) mappingContext
				.getPersistentEntity(typeToUse);
		if (persistentEntity == null) {
			throw new MappingException("No mapping metadata found for " + rawType.getName());
		}

		return readEntityFromRow(persistentEntity, row);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	protected <S> S readEntityFromRow(final CassandraPersistentEntity<S> entity, final Row row) {

		DefaultSpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(row, spELContext);
		BasicCassandraRowValueProvider rowValueProvider = new BasicCassandraRowValueProvider(row, evaluator);

		CassandraPersistentEntityParameterValueProvider parameterProvider = new CassandraPersistentEntityParameterValueProvider(
				entity, rowValueProvider, null);

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterProvider);

		readPropertiesFromRow(entity, rowValueProvider, getConvertingAccessor(instance, entity));

		return instance;
	}

	protected void readPropertiesFromRow(final CassandraPersistentEntity<?> entity,
			final BasicCassandraRowValueProvider row, final PersistentPropertyAccessor accessor) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				MappingCassandraConverter.this.readPropertyFromRow(entity, prop, row, accessor);
			}
		});
	}

	protected void readPropertyFromRow(final CassandraPersistentEntity<?> entity, final CassandraPersistentProperty prop,
			final BasicCassandraRowValueProvider row, final PersistentPropertyAccessor accessor) {

		if (entity.isConstructorArgument(prop)) { // skip 'cause prop was set in ctor
			return;
		}

		if (prop.isCompositePrimaryKey()) {

			// get the key
			CassandraPersistentProperty keyProperty = entity.getIdProperty();
			CassandraPersistentEntity<?> keyEntity = keyProperty.getCompositePrimaryKeyEntity();

			Object key = accessor.getProperty(keyProperty);

			if (key == null) {
				key = instantiatePrimaryKey(keyEntity, keyProperty, row);
			}

			// now recurse on using the key this time
			readPropertiesFromRow(prop.getCompositePrimaryKeyEntity(), row, getConvertingAccessor(key, keyEntity));

			// now that the key's properties have been populated, set the key property on the entity
			accessor.setProperty(keyProperty, key);
			return;
		}

		if (!row.getRow().getColumnDefinitions().contains(prop.getColumnName().toCql())) {
			return;
		}

		Object obj = row.getPropertyValue(prop);
		accessor.setProperty(prop, obj);
	}

	protected Object instantiatePrimaryKey(CassandraPersistentEntity<?> entity, CassandraPersistentProperty keyProperty,
			BasicCassandraRowValueProvider propertyProvider) {

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);

		return instantiator.createInstance(entity,
				new CassandraPersistentEntityParameterValueProvider(entity, propertyProvider, null));
	}

	@Override
	public <R> R read(Class<R> type, Object row) {
		if (row instanceof Row) {
			return readRow(type, (Row) row);
		}
		throw new MappingException("Unknown row object " + row.getClass().getName());
	}

	@Override
	public void write(Object source, Object sink) {

		if (source == null) {
			return;
		}

		Class<?> beanClassLoaderClass = transformClassToBeanClassLoaderClass(source.getClass());
		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(beanClassLoaderClass);

		if (entity == null) {
			throw new MappingException("No mapping metadata found for " + source.getClass());
		}

		if (sink instanceof Insert) {
			writeInsertFromObject(source, (Insert) sink, entity);
		} else if (sink instanceof Update) {
			writeUpdateFromObject(source, (Update) sink, entity);
		} else if (sink instanceof Where) {
			writeDeleteWhereFromObject(source, (Where) sink, entity);
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
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				Object value = accessor.getProperty(prop,
						prop.isCompositePrimaryKey() ? prop.getType() : CodecRegistry.DEFAULT_INSTANCE.codecFor(prop.getDataType()).getJavaType().getRawType());

				if (log.isDebugEnabled()) {
					log.debug("doWithProperties Property.type {}, Property.value {}", prop.getType().getName(), value);
				}

				if (prop.isCompositePrimaryKey()) {
					if (log.isDebugEnabled()) {
						log.debug("Property is a compositeKey");
					}

					writeInsertFromWrapper(getConvertingAccessor(value, prop.getCompositePrimaryKeyEntity()), insert,
							prop.getCompositePrimaryKeyEntity());
					return;
				}

				if (value != null) {
					if (log.isDebugEnabled()) {
						log.debug("Adding insert.value [{}] - [{}]", prop.getColumnName().toCql(), value);
					}
					insert.value(prop.getColumnName().toCql(), value);
				}
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
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				Object value = accessor.getProperty(prop,
						prop.isCompositePrimaryKey() ? prop.getType() : CodecRegistry.DEFAULT_INSTANCE.codecFor(prop.getDataType()).getJavaType().getRawType());

				if (prop.isCompositePrimaryKey()) {
					CassandraPersistentEntity<?> keyEntity = prop.getCompositePrimaryKeyEntity();
					writeUpdateFromWrapper(getConvertingAccessor(value, keyEntity), update, keyEntity);
					return;
				}

				if (value != null) {
					if (prop.isIdProperty() || entity.isCompositePrimaryKey() || prop.isPrimaryKeyColumn()) {
						update.where(QueryBuilder.eq(prop.getColumnName().toCql(), value));
					} else {
						update.with(QueryBuilder.set(prop.getColumnName().toCql(), value));
					}
				}
			}
		});
	}

	protected void writeDeleteWhereFromObject(final Object object, final Where where,
			CassandraPersistentEntity<?> entity) {
		writeDeleteWhereFromWrapper(getConvertingAccessor(object, entity), where, entity);
	}

	protected void writeDeleteWhereFromWrapper(final ConvertingPropertyAccessor accessor, final Where where,
			CassandraPersistentEntity<?> entity) {

		// if the entity itself if a composite primary key, then we've recursed, so just add columns & return
		if (entity.isCompositePrimaryKey()) {
			entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
				@Override
				public void doWithPersistentProperty(CassandraPersistentProperty prop) {

					Object value = accessor.getProperty(prop, CodecRegistry.DEFAULT_INSTANCE.codecFor(prop.getDataType()).getJavaType().getRawType());
					where.and(QueryBuilder.eq(prop.getColumnName().toCql(), value));
				}
			});
			return;
		}

		// else, wrapper is an entity with an id
		Object id = getId(accessor, entity);
		if (id == null) {
			String msg = String.format("no id value found in object %s", accessor.getBean());
			log.error(msg);
			throw new IllegalArgumentException(msg);
		}

		if (id instanceof MapId) {

			for (Map.Entry<String, Serializable> entry : ((MapId) id).entrySet()) {
				where.and(QueryBuilder.eq(entry.getKey(), entry.getValue()));
			}
			return;
		}

		CassandraPersistentProperty idProperty = entity.getIdProperty();

		if (idProperty != null) {

			if (idProperty.isCompositePrimaryKey()) {

				CassandraPersistentEntity<?> idEntity = idProperty.getCompositePrimaryKeyEntity();

				writeDeleteWhereFromWrapper(getConvertingAccessor(id, idEntity), where,
						idProperty.getCompositePrimaryKeyEntity());
				return;
			}

			where.and(QueryBuilder.eq(idProperty.getColumnName().toCql(), id));
			return;
		}
	}

	@Override
	public Object getId(Object object, CassandraPersistentEntity<?> entity) {

		Assert.notNull(object);

		final ConvertingPropertyAccessor wrapper = getConvertingAccessor(object, entity);
		object = wrapper.getBean();

		if (!entity.getType().isAssignableFrom(object.getClass())) {
			throw new IllegalArgumentException(
					String.format("given instance of type [%s] is not of compatible expected type [%s]",
							object.getClass().getName(), entity.getType().getName()));
		}

		if (object instanceof MapIdentifiable) {
			return ((MapIdentifiable) object).getMapId();
		}

		CassandraPersistentProperty idProperty = entity.getIdProperty();
		if (idProperty != null) {
			return wrapper.getProperty(entity.getIdProperty(),
					idProperty.isCompositePrimaryKey() ? idProperty.getType() : CodecRegistry.DEFAULT_INSTANCE.codecFor(idProperty.getDataType()).getJavaType().getRawType());
		}

		// if the class doesn't have an id property, then it's using MapId
		final MapId id = id();
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty p) {
				if (p.isPrimaryKeyColumn()) {
					id.with(p.getName(), (Serializable) wrapper.getProperty(p, p.getType()));
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
	 * @return
	 */
	private ConvertingPropertyAccessor getConvertingAccessor(Object source, CassandraPersistentEntity<?> entity) {

		PersistentPropertyAccessor accessor = source instanceof PersistentPropertyAccessor
				? (PersistentPropertyAccessor) source : entity.getPropertyAccessor(source);

		return new ConvertingPropertyAccessor(accessor, conversionService);
	}
}
