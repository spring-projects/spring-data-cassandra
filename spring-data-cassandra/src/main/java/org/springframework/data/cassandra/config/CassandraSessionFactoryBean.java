/*
 * Copyright 2013-2018 the original author or authors.
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

import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaCreator;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaDropper;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

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

	private @Nullable CassandraAdminOperations admin;

	private @Nullable CassandraConverter converter;

	private SchemaAction schemaAction = SchemaAction.NONE;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.config.CassandraCqlSessionFactoryBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {

		Assert.state(this.converter != null, "Converter was not properly initialized");

		super.afterPropertiesSet();

		this.admin = new CassandraAdminTemplate(getSession(), this.converter);

		performSchemaAction();
	}

	/**
	 * Perform the configure {@link SchemaAction} using {@link CassandraMappingContext} metadata.
	 */
	protected void performSchemaAction() {

		boolean create = false;
		boolean drop = DEFAULT_DROP_TABLES;
		boolean dropUnused = DEFAULT_DROP_UNUSED_TABLES;
		boolean ifNotExists = DEFAULT_CREATE_IF_NOT_EXISTS;

		switch (this.schemaAction) {
			case RECREATE_DROP_UNUSED:
				dropUnused = true;
			case RECREATE:
				drop = true;
			case CREATE_IF_NOT_EXISTS:
				ifNotExists = SchemaAction.CREATE_IF_NOT_EXISTS.equals(this.schemaAction);
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

	/**
	 * Set the {@link CassandraConverter} to use. Schema actions will derive table and user type information from the
	 * {@link CassandraMappingContext} inside {@code converter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public void setConverter(CassandraConverter converter) {

		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
	}

	/**
	 * @return the {@link CassandraConverter}.
	 */
	@Nullable
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/**
	 * @return the {@link CassandraMappingContext}.
	 */
	protected CassandraMappingContext getMappingContext() {

		CassandraConverter converter = getConverter();

		Assert.state(converter != null, "CassandraConverter was not properly initialized");

		return converter.getMappingContext();
	}

	/**
	 * Set the {@link SchemaAction}.
	 *
	 * @param schemaAction must not be {@literal null}.
	 */
	public void setSchemaAction(SchemaAction schemaAction) {

		Assert.notNull(schemaAction, "SchemaAction must not be null");
		this.schemaAction = schemaAction;
	}

	/**
	 * @return the {@link SchemaAction}.
	 */
	public SchemaAction getSchemaAction() {
		return this.schemaAction;
	}

	/**
	 * Perform schema actions.
	 *
	 * @param drop {@literal true} to drop types/tables.
	 * @param dropUnused {@literal true} to drop unused types/tables (i.e. types/tables not know to be used by
	 *          {@link CassandraMappingContext}).
	 * @param ifNotExists {@literal true} to perform creations fail-safe by adding {@code IF NOT EXISTS} to each creation
	 *          statement.
	 */
	protected void createTables(boolean drop, boolean dropUnused, boolean ifNotExists) {
		performSchemaActions(drop, dropUnused, ifNotExists);
	}

	private void performSchemaActions(boolean drop, boolean dropUnused, boolean ifNotExists) {

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(
				getMappingContext(), getCassandraAdminOperations());

		if (drop) {

			CassandraPersistentEntitySchemaDropper schemaDropper = new CassandraPersistentEntitySchemaDropper(
					getMappingContext(), getCassandraAdminOperations());

			schemaDropper.dropTables(dropUnused);
			schemaDropper.dropUserTypes(dropUnused);
		}

		schemaCreator.createUserTypes(ifNotExists);
		schemaCreator.createTables(ifNotExists);
		schemaCreator.createIndexes(ifNotExists);
	}

	/**
	 * @return the {@link CassandraAdminOperations}.
	 */
	protected CassandraAdminOperations getCassandraAdminOperations() {

		Assert.state(this.admin != null, "CassandraAdminOperations was not properly initialized");

		return this.admin;
	}
}
