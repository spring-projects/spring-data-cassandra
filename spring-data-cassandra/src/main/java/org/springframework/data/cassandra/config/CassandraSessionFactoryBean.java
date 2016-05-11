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

package org.springframework.data.cassandra.config;

import static org.springframework.cassandra.core.cql.CqlIdentifier.*;

import java.util.Collection;

import org.springframework.cassandra.config.CassandraCqlSessionFactoryBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Factory to create and configure a Cassandra {@link com.datastax.driver.core.Session} with support
 * for executing CQL and initializing the database schema (a.k.a. keyspace).
 *
 * @author Mathew Adams
 * @author David Webb
 * @author John Blum
 * @see com.datastax.driver.core.KeyspaceMetadata
 * @see com.datastax.driver.core.TableMetadata
 */
public class CassandraSessionFactoryBean extends CassandraCqlSessionFactoryBean {

	protected static final boolean DEFAULT_CREATE_IF_NOT_EXISTS = false;
	protected static final boolean DEFAULT_DROP_TABLES = false;
	protected static final boolean DEFAULT_DROP_UNUSED_TABLES = false;

	private CassandraAdminOperations admin;

	private CassandraConverter converter;

	private SchemaAction schemaAction = SchemaAction.NONE;

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();

		Assert.state(converter != null, "Converter was not properly initialized");

		admin = newCassandraAdminOperations(getObject(), converter);

		performSchemaAction();
	}

	/* (non-Javadoc) */
	CassandraAdminOperations newCassandraAdminOperations(Session session, CassandraConverter converter) {
		return new CassandraAdminTemplate(session, converter);
	}

	/* (non-Javadoc) */
	protected void performSchemaAction() {

		boolean dropTables = DEFAULT_DROP_TABLES;
		boolean dropUnused = DEFAULT_DROP_UNUSED_TABLES;
		boolean ifNotExists = DEFAULT_CREATE_IF_NOT_EXISTS;

		switch (schemaAction) {
			case RECREATE_DROP_UNUSED:
				dropUnused = true;
			case RECREATE:
				dropTables = true;
			case CREATE_IF_NOT_EXISTS:
				ifNotExists = SchemaAction.CREATE_IF_NOT_EXISTS.equals(schemaAction);
			case CREATE:
				createTables(dropTables, dropUnused, ifNotExists);
			case NONE:
			default:
				// do nothing
		}
	}

	/* (non-Javadoc) */
	protected void createTables(boolean dropTables, boolean dropUnused, boolean ifNotExists) {

		if (dropTables) {
			dropTables(dropUnused);
		}

		Collection<? extends CassandraPersistentEntity<?>> entities =
			getConverter().getMappingContext().getNonPrimaryKeyEntities();

		for (CassandraPersistentEntity<?> entity : entities) {
			// TODO: pass specification of user configurable table options
			getCassandraAdminOperations().createTable(ifNotExists, entity.getTableName(), entity.getType(), null);
		}
	}

	/* (non-Javadoc) */
	@SuppressWarnings("all")
	protected void dropTables(boolean dropUnused) {

		String keyspaceName = getKeyspaceName();

		Metadata clusterMetadata = getSession().getCluster().getMetadata();
		KeyspaceMetadata keyspaceMetadata = clusterMetadata.getKeyspace(keyspaceName);

		// TODO: fix this with KeyspaceIdentifier
		keyspaceMetadata = (keyspaceMetadata != null ? keyspaceMetadata
			: clusterMetadata.getKeyspace(keyspaceName.toLowerCase()));

		Assert.state(keyspaceMetadata != null, String.format("keyspace [%s] does not exist", keyspaceName));

		for (TableMetadata table : keyspaceMetadata.getTables()) {
			if (dropUnused || getMappingContext().usesTable(table)) {
				getCassandraAdminOperations().dropTable(cqlId(table.getName()));
			}
		}
	}

	/* (non-Javadoc) */
	protected CassandraAdminOperations getCassandraAdminOperations() {
		return this.admin;
	}

	/* (non-Javadoc) */
	public void setConverter(CassandraConverter converter) {
		Assert.notNull(converter, "CassandraConverter must not be null");
		this.converter = converter;
	}

	/* (non-Javadoc) */
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/* (non-Javadoc) */
	protected CassandraMappingContext getMappingContext() {
		return getConverter().getMappingContext();
	}

	/* (non-Javadoc) */
	public void setSchemaAction(SchemaAction schemaAction) {
		Assert.notNull(schemaAction, "SchemaAction must not be null");
		this.schemaAction = schemaAction;
	}

	/* (non-Javadoc) */
	public SchemaAction getSchemaAction() {
		return schemaAction;
	}
}
