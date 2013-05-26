/*
 * Copyright 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.AbstractMap;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

/**
 * Default implementation of a {@link MappingContext} for MongoDB using {@link BasicCassandraPersistentEntity} and
 * {@link BasicCassandraPersistentProperty} as primary abstractions.
 * 
 * @author David Webb
 */
public class CassandraMappingContext extends AbstractMappingContext<BasicCassandraPersistentEntity<?>, CassandraPersistentProperty>
		implements ApplicationContextAware {

	private static final ColumnNamingStrategy DEFAULT_NAMING_STRATEGY = PropertyNameColumnNamingStrategy.INSTANCE;

	private ColumnNamingStrategy fieldNamingStrategy = DEFAULT_NAMING_STRATEGY;
	private ApplicationContext context;

	/**
	 * Creates a new {@link CassandraMappingContext}.
	 */
	public CassandraMappingContext() {
		setSimpleTypeHolder(CassandraSimpleTypes.HOLDER);
	}

	/**
	 * Configures the {@link ColumnNamingStrategy} to be used to determine the field name if no manual mapping is applied.
	 * Defaults to a strategy using the plain property name.
	 * 
	 * @param fieldNamingStrategy the {@link ColumnNamingStrategy} to be used to determine the field name if no manual
	 *          mapping is applied.
	 */
	public void setFieldNamingStrategy(ColumnNamingStrategy fieldNamingStrategy) {
		this.fieldNamingStrategy = fieldNamingStrategy == null ? DEFAULT_NAMING_STRATEGY : fieldNamingStrategy;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.AbstractMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.mapping.MutablePersistentEntity, org.springframework.data.mapping.SimpleTypeHolder)
	 */
	@Override
	public CassandraPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			BasicCassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new CachingCassandraPersistentProperty(field, descriptor, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.BasicMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation, org.springframework.data.mapping.model.MappingContext)
	 */
	@Override
	protected <T> BasicCassandraPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		BasicCassandraPersistentEntity<T> entity = new BasicCassandraPersistentEntity<T>(typeInformation);

		if (context != null) {
			entity.setApplicationContext(context);
		}

		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}
}
