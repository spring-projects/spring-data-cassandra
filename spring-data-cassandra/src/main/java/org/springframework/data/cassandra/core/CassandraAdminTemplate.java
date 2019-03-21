/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import java.util.Map;

import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropUserTypeCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.cassandra.core.keyspace.DropUserTypeSpecification;
import org.springframework.cassandra.core.session.SessionFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Default implementation of {@link CassandraAdminOperations}.
 *
 * @author Mark Paluch
 * @author Fabio J. Mendes
 * @author John Blum
 */
public class CassandraAdminTemplate extends CassandraTemplate implements CassandraAdminOperations {

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param session must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraAdminTemplate(Session session, CassandraConverter converter) {
		super(session, converter);
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param sessionFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraAdminTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
		super(sessionFactory, converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#createTable(boolean, org.springframework.cassandra.core.cql.CqlIdentifier, java.lang.Class, java.util.Map)
	 */
	@Override
	public void createTable(boolean ifNotExists, CqlIdentifier tableName, Class<?> entityClass,
			Map<String, Object> optionsByName) {

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

		CreateTableSpecification createTableSpecification = getConverter().getMappingContext()
				.getCreateTableSpecificationFor(entity).ifNotExists(ifNotExists);

		getCqlOperations().execute(CreateTableCqlGenerator.toCql(createTableSpecification));
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
		getCqlOperations().execute(DropTableCqlGenerator.toCql(DropTableSpecification.dropTable(tableName)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#dropUserType(org.springframework.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public void dropUserType(CqlIdentifier typeName) {

		Assert.notNull(typeName, "Type name must not be null");

		getCqlOperations().execute(DropUserTypeCqlGenerator.toCql(DropUserTypeSpecification.dropType(typeName)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#getTableMetadata(java.lang.String, org.springframework.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public TableMetadata getTableMetadata(String keyspace, CqlIdentifier tableName) {

		Assert.hasText(keyspace, "Keyspace name must not be empty");
		Assert.notNull(tableName, "Table name must not be null");

		return getCqlOperations().execute((SessionCallback<TableMetadata>) session -> session.getCluster().getMetadata()
				.getKeyspace(keyspace).getTable(tableName.toCql()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#getKeyspaceMetadata()
	 */
	@Override
	public KeyspaceMetadata getKeyspaceMetadata() {

		return getCqlOperations().execute(new SessionCallback<KeyspaceMetadata>() {

			@Override
			public KeyspaceMetadata doInSession(Session session) throws DataAccessException {

				KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata()
						.getKeyspace(session.getLoggedKeyspace());

				Assert.state(keyspaceMetadata != null,
						String.format("Metadata for keyspace [%s] not available", session.getLoggedKeyspace()));

				return keyspaceMetadata;
			}
		});
	}
}
