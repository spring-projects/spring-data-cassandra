/*
 * Copyright 2013-2016 the original author or authors.
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
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropUserTypeCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.cassandra.core.keyspace.DropUserTypeSpecification;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.util.CqlUtils;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;

/**
 * Default implementation of {@link CassandraAdminOperations}.
 *
 * @author Mark Paluch
 * @author Fabio J. Mendes
 */
public class CassandraAdminTemplate extends CassandraTemplate implements CassandraAdminOperations {

	private static final Logger log = LoggerFactory.getLogger(CassandraAdminTemplate.class);

	/**
	 * Constructor used for a basic template configuration
	 *
	 * @param session must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraAdminTemplate(Session session, CassandraConverter converter) {
		super(session, converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#createTable(boolean, org.springframework.cassandra.core.cql.CqlIdentifier, java.lang.Class, java.util.Map)
	 */
	@Override
	public void createTable(final boolean ifNotExists, final CqlIdentifier tableName, Class<?> entityClass,
			Map<String, Object> optionsByName) {

		final CassandraPersistentEntity<?> entity = getCassandraMappingContext().getPersistentEntity(entityClass);

		execute(new SessionCallback<Object>() {
			@Override
			public Object doInSession(Session s) throws DataAccessException {

				String cql = new CreateTableCqlGenerator(
						getCassandraMappingContext().getCreateTableSpecificationFor(entity).ifNotExists(ifNotExists)).toCql();

				log.debug(cql);

				s.execute(cql);
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#alterTable(org.springframework.cassandra.core.cql.CqlIdentifier, java.lang.Class, boolean)
	 */
	@Override
	public void alterTable(CqlIdentifier tableName, Class<?> entityClass, boolean dropRemovedAttributeColumns) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#replaceTable(org.springframework.cassandra.core.cql.CqlIdentifier, java.lang.Class, java.util.Map)
	 */
	@Override
	public void replaceTable(CqlIdentifier tableName, Class<?> entityClass, Map<String, Object> optionsByName) {

		dropTable(tableName);
		createTable(false, tableName, entityClass, optionsByName);
	}

	/**
	 * Create a list of query operations to alter the table for the given entity
	 *
	 * @param entityClass
	 * @param tableName
	 */
	protected void doAlterTable(Class<?> entityClass, String keyspace, CqlIdentifier tableName) {

		CassandraPersistentEntity<?> entity = getCassandraMappingContext().getPersistentEntity(entityClass);

		Assert.notNull(entity, "Entity must not be null!");

		final TableMetadata tableMetadata = getTableMetadata(keyspace, tableName);
		final List<String> queryList = CqlUtils.alterTable(tableName.toCql(), entity, tableMetadata);

		execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

				for (String q : queryList) {
					log.info(q);
					s.execute(q);
				}

				return null;
			}
		});
	}

	public void dropTable(Class<?> entityClass) {
		dropTable(getTableName(entityClass));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#dropTable(org.springframework.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public void dropTable(CqlIdentifier tableName) {

		Assert.notNull(tableName, "Table name must not be null");

		log.info("Dropping table => " + tableName);

		execute(DropTableSpecification.dropTable(tableName));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#dropUserType(org.springframework.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public void dropUserType(CqlIdentifier typeName) {

		Assert.notNull(typeName, "Type name must not be null");

		log.info("Dropping user type => {}", typeName);

		execute(DropUserTypeCqlGenerator.toCql(DropUserTypeSpecification.dropType(typeName)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#getTableMetadata(java.lang.String, org.springframework.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public TableMetadata getTableMetadata(final String keyspace, final CqlIdentifier tableName) {

		Assert.hasText(keyspace, "Keyspace name must not be empty");
		Assert.notNull(tableName, "Table name must not be null");

		return execute(new SessionCallback<TableMetadata>() {
			@Override
			public TableMetadata doInSession(Session s) {
				return s.getCluster().getMetadata().getKeyspace(keyspace).getTable(tableName.toCql());
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#getKeyspaceMetadata()
	 */
	@Override
	public KeyspaceMetadata getKeyspaceMetadata() {

		return execute(new SessionCallback<KeyspaceMetadata>() {

			@Override
			public KeyspaceMetadata doInSession(Session s) throws DataAccessException {

				KeyspaceMetadata keyspaceMetadata = s.getCluster().getMetadata().getKeyspace(s.getLoggedKeyspace());
				Assert.state(keyspaceMetadata != null,
						String.format("Metadata for keyspace [%s] not available", s.getLoggedKeyspace()));
				return keyspaceMetadata;
			}
		});
	}
}
