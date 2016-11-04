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

import org.springframework.cassandra.config.CassandraCqlSessionFactoryBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaCreator;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * Factory to create and configure a Cassandra {@link com.datastax.driver.core.Session} with support for executing CQL
 * and initializing the database schema (a.k.a. keyspace).
 *
 * @author Mathew Adams
 * @author David Webb
 * @author John Blum
 * @author Mark Paluch
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

		boolean drop = DEFAULT_DROP_TABLES;
		boolean dropUnused = DEFAULT_DROP_UNUSED_TABLES;
		boolean ifNotExists = DEFAULT_CREATE_IF_NOT_EXISTS;
		boolean create = false;

		switch (schemaAction) {
			case RECREATE_DROP_UNUSED:
				dropUnused = true;
			case RECREATE:
				drop = true;
			case CREATE_IF_NOT_EXISTS:
				ifNotExists = SchemaAction.CREATE_IF_NOT_EXISTS.equals(schemaAction);
			case CREATE:
				create = true;
			case NONE:
			default:
				// do nothing
		}

		if (create) {
			createTables(drop, dropUnused, ifNotExists);
		}
	}

	protected void createTables(boolean drop, boolean dropUnused, boolean ifNotExists) {

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(
				getMappingContext(), getCassandraAdminOperations());

		schemaCreator.createUserTypes(drop, dropUnused, ifNotExists);
		schemaCreator.createTables(drop, dropUnused, ifNotExists);
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
