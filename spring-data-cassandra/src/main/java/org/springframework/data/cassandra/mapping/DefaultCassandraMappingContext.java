/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import static org.springframework.cassandra.core.keyspace.CreateTableSpecification.createTable;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.TableMetadata;

/**
 * Default implementation of a {@link MappingContext} for Cassandra using {@link CassandraPersistentEntity} and
 * {@link CassandraPersistentProperty} as primary abstractions.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class DefaultCassandraMappingContext extends
		AbstractMappingContext<CassandraPersistentEntity<?>, CassandraPersistentProperty> implements
		CassandraMappingContext, ApplicationContextAware {

	protected ApplicationContext context;
	protected Map<String, Set<CassandraPersistentEntity<?>>> entitySetsByTableName = new HashMap<String, Set<CassandraPersistentEntity<?>>>();

	/**
	 * Creates a new {@link DefaultCassandraMappingContext}.
	 */
	public DefaultCassandraMappingContext() {
		setSimpleTypeHolder(new CassandraSimpleTypeHolder());
	}

	@Override
	public CassandraPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			CassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {

		return createPersistentProperty(field, descriptor, owner, (CassandraSimpleTypeHolder) simpleTypeHolder);
	}

	public CassandraPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			CassandraPersistentEntity<?> owner, CassandraSimpleTypeHolder simpleTypeHolder) {

		return new CachingCassandraPersistentProperty(field, descriptor, owner, simpleTypeHolder);
	}

	@Override
	protected <T> CassandraPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		CassandraPersistentEntity<T> entity = new CachingCassandraPersistentEntity<T>(typeInformation, this);

		if (context != null) {
			entity.setApplicationContext(context);
		}

		Set<CassandraPersistentEntity<?>> entities = entitySetsByTableName.get(entity.getTableName());
		if (entities == null) {
			entities = new HashSet<CassandraPersistentEntity<?>>();
		}
		entities.add(entity);
		entitySetsByTableName.put(entity.getTableName(), entities);

		return entity;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}

	@Override
	public boolean usesTable(TableMetadata table) {
		return entitySetsByTableName.containsKey(table.getName());
	}

	@Override
	public CreateTableSpecification getCreateTableSpecificationFor(CassandraPersistentEntity<?> entity) {

		Assert.notNull(entity);

		final CreateTableSpecification spec = createTable().name(entity.getTableName());

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isCompositePrimaryKey()) {

					CassandraPersistentEntity<?> pkEntity = getPersistentEntity(prop.getRawType());

					pkEntity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {

						@Override
						public void doWithPersistentProperty(CassandraPersistentProperty pkProp) {

							if (pkProp.isPartitionKeyColumn()) {
								spec.partitionKeyColumn(pkProp.getColumnName(), pkProp.getDataType());
							} else { // it's a cluster column
								spec.clusteredKeyColumn(pkProp.getColumnName(), pkProp.getDataType(), pkProp.getPrimaryKeyOrdering());
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
			throw new MappingException("no partition key columns found in the entity " + entity.getType());
		}

		return spec;
	}
}
