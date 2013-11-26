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
package org.springframework.data.cassandra.core;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.config.KeyspaceAttributes;
import org.springframework.data.cassandra.config.TableAttributes;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.util.CqlUtils;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Convenient factory for configuring a Cassandra Session. Session is a thread safe singleton and created per a
 * keyspace. So, it is enough to have one session per application.
 * 
 * @author Alex Shvid
 */

public class CassandraKeyspaceFactoryBean implements FactoryBean<Keyspace>, InitializingBean, DisposableBean,
		BeanClassLoaderAware, PersistenceExceptionTranslator {

	private static final Logger log = LoggerFactory.getLogger(CassandraKeyspaceFactoryBean.class);

	public static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
	public static final int DEFAULT_REPLICATION_FACTOR = 1;

	private ClassLoader beanClassLoader;

	private Cluster cluster;
	private Session session;
	private String keyspace;

	private CassandraConverter converter;
	private MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private Keyspace keyspaceBean;

	private KeyspaceAttributes keyspaceAttributes;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	public Keyspace getObject() {
		return keyspaceBean;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends Session> getObjectType() {
		return Session.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {

		if (this.converter == null) {
			this.converter = getDefaultCassandraConverter();
		}
		this.mappingContext = this.converter.getMappingContext();

		if (cluster == null) {
			throw new IllegalArgumentException("at least one cluster is required");
		}

		Session session = null;
		session = cluster.connect();

		if (StringUtils.hasText(keyspace)) {

			KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace.toLowerCase());
			boolean keyspaceExists = keyspaceMetadata != null;
			boolean keyspaceCreated = false;

			if (keyspaceExists) {
				log.info("keyspace exists " + keyspaceMetadata.asCQLQuery());
			}

			if (keyspaceAttributes == null) {
				keyspaceAttributes = new KeyspaceAttributes();
			}

			// drop the old keyspace if needed
			if (keyspaceExists && (keyspaceAttributes.isCreate() || keyspaceAttributes.isCreateDrop())) {
				log.info("Drop keyspace " + keyspace + " on afterPropertiesSet");
				session.execute("DROP KEYSPACE " + keyspace + ";");
				keyspaceExists = false;
			}

			// create the new keyspace if needed
			if (!keyspaceExists
					&& (keyspaceAttributes.isCreate() || keyspaceAttributes.isCreateDrop() || keyspaceAttributes.isUpdate())) {

				String query = String
						.format(
								"CREATE KEYSPACE %1$s WITH replication = { 'class' : '%2$s', 'replication_factor' : %3$d } AND DURABLE_WRITES = %4$b",
								keyspace, keyspaceAttributes.getReplicationStrategy(), keyspaceAttributes.getReplicationFactor(),
								keyspaceAttributes.isDurableWrites());

				log.info("Create keyspace " + keyspace + " on afterPropertiesSet " + query);

				session.execute(query);
				keyspaceCreated = true;
			}

			// update keyspace if needed
			if (keyspaceAttributes.isUpdate() && !keyspaceCreated) {

				if (compareKeyspaceAttributes(keyspaceAttributes, keyspaceMetadata) != null) {

					String query = String
							.format(
									"ALTER KEYSPACE %1$s WITH replication = { 'class' : '%2$s', 'replication_factor' : %3$d } AND DURABLE_WRITES = %4$b",
									keyspace, keyspaceAttributes.getReplicationStrategy(), keyspaceAttributes.getReplicationFactor(),
									keyspaceAttributes.isDurableWrites());

					log.info("Update keyspace " + keyspace + " on afterPropertiesSet " + query);
					session.execute(query);
				}

			}

			// validate keyspace if needed
			if (keyspaceAttributes.isValidate()) {

				if (!keyspaceExists) {
					throw new InvalidDataAccessApiUsageException("keyspace '" + keyspace + "' not found in the Cassandra");
				}

				String errorField = compareKeyspaceAttributes(keyspaceAttributes, keyspaceMetadata);
				if (errorField != null) {
					throw new InvalidDataAccessApiUsageException(errorField + " attribute is not much in the keyspace '"
							+ keyspace + "'");
				}

			}

			session.execute("USE " + keyspace);

			if (!CollectionUtils.isEmpty(keyspaceAttributes.getTables())) {

				for (TableAttributes tableAttributes : keyspaceAttributes.getTables()) {

					String entityClassName = tableAttributes.getEntity();
					Class<?> entityClass = ClassUtils.forName(entityClassName, this.beanClassLoader);
					CassandraPersistentEntity<?> entity = determineEntity(entityClass);
					String useTableName = tableAttributes.getName() != null ? tableAttributes.getName() : entity.getTable();

					if (keyspaceCreated) {
						createNewTable(session, useTableName, entity);
					} else if (keyspaceAttributes.isUpdate()) {
						TableMetadata table = keyspaceMetadata.getTable(useTableName.toLowerCase());
						if (table == null) {
							createNewTable(session, useTableName, entity);
						} else {
							// alter table columns
							for (String cql : CqlUtils.alterTable(useTableName, entity, table)) {
								log.info("Execute on keyspace " + keyspace + " CQL " + cql);
								session.execute(cql);
							}
						}
					} else if (keyspaceAttributes.isValidate()) {
						TableMetadata table = keyspaceMetadata.getTable(useTableName.toLowerCase());
						if (table == null) {
							throw new InvalidDataAccessApiUsageException("not found table " + useTableName + " for entity "
									+ entityClassName);
						}
						// validate columns
						List<String> alter = CqlUtils.alterTable(useTableName, entity, table);
						if (!alter.isEmpty()) {
							throw new InvalidDataAccessApiUsageException("invalid table " + useTableName + " for entity "
									+ entityClassName + ". modify it by " + alter);
						}
					}

					// System.out.println("tableAttributes, entityClass=" + entityClass + ", table = " + entity.getTable());

				}
			}

		}

		// initialize property
		this.session = session;

		this.keyspaceBean = new Keyspace(keyspace, session, converter);
	}

	private void createNewTable(Session session, String useTableName, CassandraPersistentEntity<?> entity)
			throws NoHostAvailableException {
		String cql = CqlUtils.createTable(useTableName, entity);
		log.info("Execute on keyspace " + keyspace + " CQL " + cql);
		session.execute(cql);
		for (String indexCQL : CqlUtils.createIndexes(useTableName, entity)) {
			log.info("Execute on keyspace " + keyspace + " CQL " + indexCQL);
			session.execute(indexCQL);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {

		if (StringUtils.hasText(keyspace) && keyspaceAttributes != null && keyspaceAttributes.isCreateDrop()) {
			log.info("Drop keyspace " + keyspace + " on destroy");
			session.execute("USE system");
			session.execute("DROP KEYSPACE " + keyspace);
		}
		this.session.shutdown();
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public void setKeyspaceAttributes(KeyspaceAttributes keyspaceAttributes) {
		this.keyspaceAttributes = keyspaceAttributes;
	}

	public void setConverter(CassandraConverter converter) {
		this.converter = converter;
	}

	private static String compareKeyspaceAttributes(KeyspaceAttributes keyspaceAttributes,
			KeyspaceMetadata keyspaceMetadata) {
		if (keyspaceAttributes.isDurableWrites() != keyspaceMetadata.isDurableWrites()) {
			return "durableWrites";
		}
		Map<String, String> replication = keyspaceMetadata.getReplication();
		String replicationFactorStr = replication.get("replication_factor");
		if (replicationFactorStr == null) {
			return "replication_factor";
		}
		try {
			int replicationFactor = Integer.parseInt(replicationFactorStr);
			if (keyspaceAttributes.getReplicationFactor() != replicationFactor) {
				return "replication_factor";
			}
		} catch (NumberFormatException e) {
			return "replication_factor";
		}

		String attributesStrategy = keyspaceAttributes.getReplicationStrategy();
		if (attributesStrategy.indexOf('.') == -1) {
			attributesStrategy = "org.apache.cassandra.locator." + attributesStrategy;
		}
		String replicationStrategy = replication.get("class");
		if (!attributesStrategy.equals(replicationStrategy)) {
			return "replication_class";
		}
		return null;
	}

	CassandraPersistentEntity<?> determineEntity(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ entityClass.getName());
		}
		return entity;
	}

	private static final CassandraConverter getDefaultCassandraConverter() {
		MappingCassandraConverter converter = new MappingCassandraConverter(new CassandraMappingContext());
		converter.afterPropertiesSet();
		return converter;
	}
}
