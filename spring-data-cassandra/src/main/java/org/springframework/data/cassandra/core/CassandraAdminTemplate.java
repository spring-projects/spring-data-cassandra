/*
 * Copyright 2013-2020 the original author or authors.
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
import java.util.Optional;

import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.SchemaFactory;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.DropUserTypeCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropUserTypeSpecification;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Default implementation of {@link CassandraAdminOperations}.
 *
 * @author Mark Paluch
 * @author Fabio J. Mendes
 * @author John Blum
 * @author Vagif Zeynalov
 */
public class CassandraAdminTemplate extends CassandraTemplate implements CassandraAdminOperations {

	protected static final boolean DEFAULT_DROP_TABLE_IF_EXISTS = false;

	private final SchemaFactory schemaFactory;

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param session must not be {@literal null}.
	 * @since 2.2
	 */
	public CassandraAdminTemplate(CqlSession session) {
		super(session);

		this.schemaFactory = new SchemaFactory(getConverter());
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param session must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraAdminTemplate(CqlSession session, CassandraConverter converter) {
		super(session, converter);
		this.schemaFactory = new SchemaFactory(getConverter());
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param sessionFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraAdminTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
		super(sessionFactory, converter);

		this.schemaFactory = new SchemaFactory(getConverter());
	}

	/**
	 * Constructor used for a basic template configuration.
	 *
	 * @param cqlOperations must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @since 2.2
	 */
	public CassandraAdminTemplate(CqlOperations cqlOperations, CassandraConverter converter) {
		super(cqlOperations, converter);

		this.schemaFactory = new SchemaFactory(getConverter());
	}

	@Override
	public SchemaFactory getSchemaFactory() {
		return schemaFactory;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#createTable(boolean, org.springframework.data.cassandra.core.cql.CqlIdentifier, java.lang.Class, java.util.Map)
	 */
	@Override
	public void createTable(boolean ifNotExists, CqlIdentifier tableName, Class<?> entityClass,
			Map<String, Object> optionsByName) {

		CassandraPersistentEntity<?> entity = getConverter().getMappingContext().getRequiredPersistentEntity(entityClass);

		CreateTableSpecification createTableSpecification = this.schemaFactory
				.getCreateTableSpecificationFor(entity, tableName).ifNotExists(ifNotExists);

		getCqlOperations().execute(CreateTableCqlGenerator.toCql(createTableSpecification));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#dropTable(java.lang.Class)
	 */
	@Override
	public void dropTable(Class<?> entityClass) {
		dropTable(getTableName(entityClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#dropTable(org.springframework.data.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public void dropTable(CqlIdentifier tableName) {
		dropTable(DEFAULT_DROP_TABLE_IF_EXISTS, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#dropTable(boolean, CqlIdentifier)
	 */
	@Override
	public void dropTable(boolean ifExists, CqlIdentifier tableName) {

		String dropTableCql = DropTableCqlGenerator.toCql(DropTableSpecification.dropTable(tableName).ifExists(ifExists));

		getCqlOperations().execute(dropTableCql);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#dropUserType(org.springframework.data.cassandra.core.cql.CqlIdentifier)
	 */
	@Override
	public void dropUserType(CqlIdentifier typeName) {

		Assert.notNull(typeName, "Type name must not be null");

		String dropUserTypeCql = DropUserTypeCqlGenerator.toCql(DropUserTypeSpecification.dropType(typeName));

		getCqlOperations().execute(dropUserTypeCql);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#getTableMetadata(com.datastax.oss.driver.api.core.CqlIdentifier, com.datastax.oss.driver.api.core.CqlIdentifier)
	 */
	@Override
	public Optional<TableMetadata> getTableMetadata(CqlIdentifier keyspace, CqlIdentifier tableName) {

		Assert.notNull(keyspace, "Keyspace name must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		// noinspection ConstantConditions
		return getCqlOperations().execute((SessionCallback<Optional<TableMetadata>>) session -> {
			return session.getMetadata().getKeyspace(keyspace).flatMap(it -> it.getTable(tableName));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#getKeyspaceMetadata()
	 */
	@Override
	public KeyspaceMetadata getKeyspaceMetadata() {

		// noinspection ConstantConditions
		return getCqlOperations().execute((SessionCallback<KeyspaceMetadata>) session -> {

			return session.getKeyspace().flatMap(it -> session.getMetadata().getKeyspace(it)).orElseThrow(() -> {
				return new IllegalStateException("Metadata for keyspace not available");
			});
		});
	}
}
