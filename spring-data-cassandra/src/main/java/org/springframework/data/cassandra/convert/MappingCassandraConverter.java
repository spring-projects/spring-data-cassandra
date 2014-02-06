/*
 * Copyright 2011-2013 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.convert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

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
 */
public class MappingCassandraConverter extends AbstractCassandraConverter implements CassandraConverter,
		ApplicationContextAware, BeanClassLoaderAware {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected final CassandraMappingContext mappingContext;
	protected ApplicationContext applicationContext;
	protected SpELContext spELContext;
	protected boolean useFieldAccessOnly = true;

	protected ClassLoader beanClassLoader;

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

		CassandraPersistentEntity<R> persistentEntity = (CassandraPersistentEntity<R>) mappingContext
				.getPersistentEntity(typeToUse);
		if (persistentEntity == null) {
			throw new MappingException("No mapping metadata found for " + rawType.getName());
		}

		return readEntityFromRow(persistentEntity, row);
	}

	@Override
	public MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	protected <S> S readEntityFromRow(final CassandraPersistentEntity<S> entity, final Row row) {

		final DefaultSpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(row, spELContext);

		final CassandraPropertyValueProvider propertyProvider = new CassandraPropertyValueProvider(row, evaluator);

		CassandraPersistentEntityParameterValueProvider parameterProvider = new CassandraPersistentEntityParameterValueProvider(
				entity, propertyProvider, null);

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterProvider);

		final BeanWrapper<CassandraPersistentEntity<S>, S> wrapper = BeanWrapper.create(instance, conversionService);

		readPropertiesFromRow(entity, row, propertyProvider, wrapper);

		return wrapper.getBean();
	}

	protected void readPropertiesFromRow(final CassandraPersistentEntity<?> entity, final Row row,
			final CassandraPropertyValueProvider propertyProvider, final BeanWrapper<?, ?> wrapper) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				MappingCassandraConverter.this.readPropertyFromRow(row, entity, prop, propertyProvider, wrapper);
			}
		});
	}

	protected void readPropertyFromRow(final Row row, final CassandraPersistentEntity<?> entity,
			final CassandraPersistentProperty prop, final CassandraPropertyValueProvider propertyProvider,
			final BeanWrapper<?, ?> wrapper) {

		if (entity.isConstructorArgument(prop)) { // skip 'cause prop was set in ctor
			return;
		}

		if (prop.isCompositePrimaryKey()) {
			readPropertiesFromRow(prop.getCompositePrimaryKeyEntity(), row, propertyProvider, wrapper);
			return;
		}

		if (!row.getColumnDefinitions().contains(prop.getColumnName())) {
			return;
		}

		Object obj = propertyProvider.getPropertyValue(prop);
		wrapper.setProperty(prop, obj, useFieldAccessOnly);
	}

	public boolean getUseFieldAccessOnly() {
		return useFieldAccessOnly;
	}

	public void setUseFieldAccessOnly(boolean useFieldAccessOnly) {
		this.useFieldAccessOnly = useFieldAccessOnly;
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
			throw new MappingException("Unknown buildStatement " + sink.getClass().getName());
		}
	}

	protected void writeInsertFromObject(final Object object, final Insert insert, CassandraPersistentEntity<?> entity) {
		writeInsertFromWrapper(BeanWrapper.<CassandraPersistentEntity<Object>, Object> create(object, conversionService),
				insert, entity);
	}

	protected void writeInsertFromWrapper(final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper,
			final Insert insert, CassandraPersistentEntity<?> entity) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				Object value = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

				if (prop.isCompositePrimaryKey()) {
					writeInsertFromWrapper(
							BeanWrapper.<CassandraPersistentEntity<Object>, Object> create(value, conversionService), insert,
							prop.getCompositePrimaryKeyEntity());
					return;
				}

				if (value != null) {
					insert.value(prop.getColumnName(), value);
				}
			}
		});
	}

	protected void writeUpdateFromObject(final Object object, final Update update, CassandraPersistentEntity<?> entity) {
		writeUpdateFromWrapper(BeanWrapper.<CassandraPersistentEntity<Object>, Object> create(object, conversionService),
				update, entity);
	}

	protected void writeUpdateFromWrapper(final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper,
			final Update update, final CassandraPersistentEntity<?> entity) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				Object value = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

				if (prop.isCompositePrimaryKey()) {
					writeUpdateFromWrapper(
							BeanWrapper.<CassandraPersistentEntity<Object>, Object> create(value, conversionService), update,
							prop.getCompositePrimaryKeyEntity());
					return;
				}

				if (value != null) {
					if (prop.isIdProperty() || entity.isCompositePrimaryKey()) {
						update.where(QueryBuilder.eq(prop.getColumnName(), value));
					} else {
						update.with(QueryBuilder.set(prop.getColumnName(), value));
					}
				}
			}
		});
	}

	protected void writeDeleteWhereFromObject(final Object object, final Where where, CassandraPersistentEntity<?> entity) {
		writeDeleteWhereFromWrapper(
				BeanWrapper.<CassandraPersistentEntity<Object>, Object> create(object, conversionService), where, entity);
	}

	protected void writeDeleteWhereFromWrapper(final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper,
			final Where where, CassandraPersistentEntity<?> entity) {

		CassandraPersistentProperty idProperty = entity.getIdProperty();
		Object idValue = wrapper.getProperty(idProperty, idProperty.getType(), useFieldAccessOnly);

		if (idValue == null) {
			String msg = String.format("no id value found in object {}", wrapper.getBean());
			log.error(msg);
			throw new IllegalArgumentException(msg);
		}

		if (idProperty.isCompositePrimaryKey()) {
			writeDeleteWhereFromWrapper(
					BeanWrapper.<CassandraPersistentEntity<Object>, Object> create(idValue, conversionService), where,
					idProperty.getCompositePrimaryKeyEntity());
			return;
		}

		where.and(QueryBuilder.eq(idProperty.getColumnName(), idValue));
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
	public CassandraMappingContext getCassandraMappingContext() {
		return mappingContext;
	}
}
