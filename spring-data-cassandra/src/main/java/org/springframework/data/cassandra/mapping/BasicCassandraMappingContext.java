/*
 * Copyright 2013-2014 the original author or authors
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
package org.springframework.data.cassandra.mapping;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;
import static org.springframework.cassandra.core.keyspace.CreateTableSpecification.createTable;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.cassandra.core.cql.CqlIdentifier;
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
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.TableMetadata;

/**
 * Default implementation of a {@link MappingContext} for Cassandra using {@link CassandraPersistentEntity} and
 * {@link CassandraPersistentProperty} as primary abstractions.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class BasicCassandraMappingContext extends
		AbstractMappingContext<CassandraPersistentEntity<?>, CassandraPersistentProperty> implements
		CassandraMappingContext, ApplicationContextAware {

	protected ApplicationContext context;
	protected Mapping mapping = new Mapping();
	protected ClassLoader beanClassLoader;
	protected CassandraPersistentEntityMetadataVerifier verifier = new BasicCassandraPersistentEntityMetadataVerifier();

	// useful caches
	protected Map<CqlIdentifier, Set<CassandraPersistentEntity<?>>> entitySetsByTableName = new HashMap<CqlIdentifier, Set<CassandraPersistentEntity<?>>>();
	protected Set<CassandraPersistentEntity<?>> nonPrimaryKeyEntities = new HashSet<CassandraPersistentEntity<?>>();
	protected Set<CassandraPersistentEntity<?>> primaryKeyEntities = new HashSet<CassandraPersistentEntity<?>>();
	protected Map<Class<?>, CassandraPersistentEntity<?>> entitiesByType = new HashMap<Class<?>, CassandraPersistentEntity<?>>();

	/**
	 * Creates a new {@link BasicCassandraMappingContext}.
	 */
	public BasicCassandraMappingContext() {
		setSimpleTypeHolder(new CassandraSimpleTypeHolder());
	}

	@Override
	public void initialize() {

		super.initialize();

		processMappingOverrides();
	}

	@Override
	public Collection<CassandraPersistentEntity<?>> getPersistentEntities() {
		return getPersistentEntities(false);
	}

	@Override
	public Collection<CassandraPersistentEntity<?>> getPrimaryKeyEntities() {
		return Collections.unmodifiableSet(primaryKeyEntities);
	}

	@Override
	public Collection<CassandraPersistentEntity<?>> getNonPrimaryKeyEntities() {
		return Collections.unmodifiableSet(nonPrimaryKeyEntities);
	}

	@Override
	public Collection<CassandraPersistentEntity<?>> getPersistentEntities(boolean includePrimaryKeyTypes) {
		if (includePrimaryKeyTypes) {
			return super.getPersistentEntities();
		}
		return Collections.unmodifiableSet(nonPrimaryKeyEntities);
	}

	@Override
	public CassandraPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			CassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {

		return createPersistentProperty(field, descriptor, owner, (CassandraSimpleTypeHolder) simpleTypeHolder);
	}

	public CassandraPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			CassandraPersistentEntity<?> owner, CassandraSimpleTypeHolder simpleTypeHolder) {

		return new BasicCassandraPersistentProperty(field, descriptor, owner, simpleTypeHolder);
	}

	@Override
	protected <T> CassandraPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		CassandraPersistentEntity<T> entity = new BasicCassandraPersistentEntity<T>(typeInformation, this, verifier);

		if (context != null) {
			entity.setApplicationContext(context);
		}

		// now do some caching of the entity

		Set<CassandraPersistentEntity<?>> entities = entitySetsByTableName.get(entity.getTableName());
		if (entities == null) {
			entities = new HashSet<CassandraPersistentEntity<?>>();
		}
		entities.add(entity);
		entitySetsByTableName.put(entity.getTableName(), entities);

		if (entity.isCompositePrimaryKey()) {
			primaryKeyEntities.add(entity);
		} else {
			nonPrimaryKeyEntities.add(entity);
		}

		entitiesByType.put(entity.getType(), entity);

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

					if (prop.isIdProperty() || prop.isPartitionKeyColumn()) {
						spec.partitionKeyColumn(prop.getColumnName(), prop.getDataType());
					} else if (prop.isClusterKeyColumn()) {
						spec.clusteredKeyColumn(prop.getColumnName(), prop.getDataType());
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

	public void setMapping(Mapping mapping) {

		Assert.notNull(mapping);

		this.mapping = mapping;
	}

	protected void processMappingOverrides() {

		if (mapping == null) {
			return;
		}

		for (EntityMapping entityMapping : mapping.getEntityMappings()) {

			if (entityMapping == null) {
				continue;
			}

			String entityClassName = entityMapping.getEntityClassName();
			Class<?> entityClass;
			try {
				entityClass = ClassUtils.forName(entityClassName, beanClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException(String.format("unknown persistent entity name [%s]", entityClassName), e);
			}

			CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

			if (entity == null) {
				throw new IllegalStateException(String.format("unknown persistent entity class name [%s]", entityClassName));
			}

			String tableName = entityMapping.getTableName();
			if (StringUtils.hasText(tableName)) {
				entity.setTableName(cqlId(tableName, Boolean.valueOf(entityMapping.getForceQuote())));
			}

			processMappingOverrides(entity, entityMapping);
		}
	}

	protected void processMappingOverrides(CassandraPersistentEntity<?> entity, EntityMapping entityMapping) {

		for (PropertyMapping mapping : entityMapping.getPropertyMappings().values()) {
			processMappingOverride(entity, mapping);
		}
	}

	protected void processMappingOverride(CassandraPersistentEntity<?> entity, PropertyMapping mapping) {

		CassandraPersistentProperty property = entity.getPersistentProperty(mapping.getPropertyName());
		if (property == null) {
			throw new IllegalArgumentException(String.format("entity class [%s] has no persistent property named [%s]",
					entity.getType().getName(), mapping.getPropertyName()));
		}

		boolean forceQuote = false;
		String value = mapping.getForceQuote();
		if (StringUtils.hasText(value)) {
			property.setForceQuote(forceQuote = Boolean.valueOf(value));
		}

		value = mapping.getColumnName();

		if (StringUtils.hasText(value)) {
			property.setColumnName(cqlId(value, forceQuote));
		}

	}

	public void setBeanClassLoader(ClassLoader beanClassLoader) {
		this.beanClassLoader = beanClassLoader;
	}

	@Override
	public CassandraPersistentEntity<?> getExistingPersistentEntity(Class<?> type) {

		CassandraPersistentEntity<?> entity = entitiesByType.get(type);
		if (entity != null) {
			return entity;
		}

		throw new IllegalArgumentException(String.format("unknown persistent type [%s]", type.getName()));
	}

	@Override
	public boolean contains(Class<?> type) {
		return entitiesByType.containsKey(type);
	}

	/**
	 * @return Returns the verifier.
	 */
	public CassandraPersistentEntityMetadataVerifier getVerifier() {
		return verifier;
	}

	/**
	 * @param verifier The verifier to set.
	 */
	@Override
	public void setVerifier(CassandraPersistentEntityMetadataVerifier verifier) {
		this.verifier = verifier;
	}
	
	@Override
	protected CassandraPersistentEntity<?> addPersistentEntity(TypeInformation<?> typeInformation) {
	    if (!verifier.isPersistent(typeInformation)) {
	        // entity is not persistent
	        return null;
	    }
	    return super.addPersistentEntity(typeInformation);
	}
}
