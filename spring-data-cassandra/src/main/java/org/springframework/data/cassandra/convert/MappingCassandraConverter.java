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
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
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
 */
public class MappingCassandraConverter extends AbstractCassandraConverter implements CassandraConverter,
		ApplicationContextAware, BeanClassLoaderAware {

	protected static final Logger log = LoggerFactory.getLogger(MappingCassandraConverter.class);

	protected final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;
	protected ApplicationContext applicationContext;
	private SpELContext spELContext;
	private boolean useFieldAccessOnly = true;

	private ClassLoader beanClassLoader;

	/**
	 * Creates a new {@link MappingCassandraConverter} given the new {@link MappingContext}.
	 * 
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingCassandraConverter(
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {
		super(new DefaultConversionService());
		this.mappingContext = mappingContext;
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
	}

	@SuppressWarnings("unchecked")
	public <R> R readRow(Class<R> clazz, Row row) {

		Class<R> beanClassLoaderClass = transformClassToBeanClassLoaderClass(clazz);

		TypeInformation<? extends R> type = ClassTypeInformation.from(beanClassLoaderClass);
		// TypeInformation<? extends R> typeToUse = typeMapper.readType(row, type);
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

		return readRowInternal(persistentEntity, row);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	public MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	protected <S extends Object> S readRowInternal(final CassandraPersistentEntity<S> entity, final Row row) {

		final DefaultSpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(row, spELContext);

		final PropertyValueProvider<CassandraPersistentProperty> propertyProvider = new CassandraPropertyValueProvider(row,
				evaluator);
		PersistentEntityParameterValueProvider<CassandraPersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<CassandraPersistentProperty>(
				entity, propertyProvider, null);

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterProvider);

		final BeanWrapper<CassandraPersistentEntity<S>, S> wrapper = BeanWrapper.create(instance, conversionService);
		S result = wrapper.getBean();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				MappingCassandraConverter.this.handlePersistentPropertyRead(row, entity, prop, propertyProvider, wrapper);
			}
		});

		return result;
	}

	protected void handlePersistentPropertyRead(final Row row, final CassandraPersistentEntity<?> entity,
			final CassandraPersistentProperty prop,
			final PropertyValueProvider<CassandraPersistentProperty> propertyProvider, final BeanWrapper<?, ?> wrapper) {

		if (entity.isConstructorArgument(prop)) { // skip 'cause prop was set in ctor
			return;
		}

		if (prop.isCompositePrimaryKey()) {
			// TODO: handle composite primary key properties via recursion into this method
		}

		boolean hasValueForProperty = row.getColumnDefinitions().contains(prop.getColumnName());
		if (!hasValueForProperty) {
			return;
		}

		Object obj = propertyProvider.getPropertyValue(prop);
		wrapper.setProperty(prop, obj, useFieldAccessOnly);
	}

	public void setUseFieldAccessOnly(boolean useFieldAccessOnly) {
		this.useFieldAccessOnly = useFieldAccessOnly;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public <R> R read(Class<R> type, Object row) {
		if (row instanceof Row) {
			return readRow(type, (Row) row);
		}
		throw new MappingException("Unknown row object " + row.getClass().getName());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(Object obj, Object builtStatement) {

		if (obj == null) {
			return;
		}

		Class<?> beanClassLoaderClass = transformClassToBeanClassLoaderClass(obj.getClass());
		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(beanClassLoaderClass);

		if (entity == null) {
			throw new MappingException("No mapping metadata found for " + obj.getClass());
		}

		if (builtStatement instanceof Insert) {
			writeInsertInternal(obj, (Insert) builtStatement, entity);
		} else if (builtStatement instanceof Update) {
			writeUpdateInternal(obj, (Update) builtStatement, entity);
		} else if (builtStatement instanceof Where) {
			writeDeleteWhereInternal(obj, (Where) builtStatement, entity);
		} else {
			throw new MappingException("Unknown buildStatement " + builtStatement.getClass().getName());
		}
	}

	private void writeInsertInternal(final Object objectToSave, final Insert insert, CassandraPersistentEntity<?> entity) {

		final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(objectToSave,
				conversionService);

		// Write the properties
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				Object propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

				if (propertyObj != null) {
					insert.value(prop.getColumnName(), propertyObj);
				}

			}
		});

	}

	private void writeUpdateInternal(final Object objectToSave, final Update update, CassandraPersistentEntity<?> entity) {

		final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(objectToSave,
				conversionService);

		// Write the properties
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				Object propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

				if (propertyObj != null) {
					if (prop.isIdProperty()) {
						update.where(QueryBuilder.eq(prop.getColumnName(), propertyObj));
					} else {
						update.with(QueryBuilder.set(prop.getColumnName(), propertyObj));
					}
				}

			}
		});

	}

	private void writeDeleteWhereInternal(final Object objectToSave, final Where whereId,
			CassandraPersistentEntity<?> entity) {

		final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(objectToSave,
				conversionService);

		// Write the properties
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isIdProperty()) {

					Object propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

					if (propertyObj != null) {
						whereId.and(QueryBuilder.eq(prop.getColumnName(), propertyObj));
					}
				}

			}
		});

	}

	public CreateTableSpecification getCreateTableSpecification(CassandraPersistentEntity<?> entity) {

		final CreateTableSpecification spec = new CreateTableSpecification();

		spec.name(entity.getTableName());

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isCompositePrimaryKey()) {

					CassandraPersistentEntity<?> pkEntity = mappingContext.getPersistentEntity(prop.getRawType());

					pkEntity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
						public void doWithPersistentProperty(CassandraPersistentProperty pkProp) {

							if (pkProp.isPartitionKeyColumn()) {
								spec.partitionKeyColumn(pkProp.getColumnName(), pkProp.getDataType());
							} else {
								spec.clusteredKeyColumn(pkProp.getColumnName(), pkProp.getDataType(), pkProp.getOrdering());
							}

						}
					});

				} else {

					if (prop.isIdProperty()) {
						spec.partitionKeyColumn(prop.getColumnName(), prop.getDataType());
					} else {
						spec.column(prop.getColumnName(), prop.getDataType());
					}

				}
			}

		});

		if (spec.getPartitionKeyColumns().isEmpty()) {
			throw new MappingException("not found partition key in the entity " + entity.getType());
		}

		return spec;

	}

	@SuppressWarnings("unchecked")
	private <T> Class<T> transformClassToBeanClassLoaderClass(Class<T> entity) {
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

}
