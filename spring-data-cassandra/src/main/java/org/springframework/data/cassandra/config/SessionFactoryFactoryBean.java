/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaCreator;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaDropper;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.data.cassandra.core.cql.session.init.KeyspacePopulator;
import org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Factory to create and configure a Cassandra {@link SessionFactory} with support for executing CQL and initializing
 * the database schema (a.k.a. keyspace). This factory bean invokes a {@link SessionFactoryInitializer} to prepare a
 * keyspace before applying {@link SchemaAction schema actions} such as creating user-defined types and tables.
 *
 * @author Mark Paluch
 * @since 3.0
 * @see SessionFactoryInitializer
 */
public class SessionFactoryFactoryBean extends AbstractFactoryBean<SessionFactory> {

	protected static final boolean DEFAULT_CREATE_IF_NOT_EXISTS = false;
	protected static final boolean DEFAULT_DROP_TABLES = false;
	protected static final boolean DEFAULT_DROP_UNUSED_TABLES = false;

	private CassandraConverter converter;

	private CqlSession session;

	private @Nullable KeyspacePopulator keyspaceCleaner;
	private @Nullable KeyspacePopulator keyspacePopulator;

	private SchemaAction schemaAction = SchemaAction.NONE;

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
	 * Set the {@link KeyspacePopulator} to execute during the bean destruction phase, cleaning up the keyspace and
	 * leaving it in a known state for others.
	 *
	 * @param keyspaceCleaner the {@link KeyspacePopulator} to use during cleanup.
	 * @see #setKeyspacePopulator
	 */
	public void setKeyspaceCleaner(@Nullable KeyspacePopulator keyspaceCleaner) {
		this.keyspaceCleaner = keyspaceCleaner;
	}

	/**
	 * Set the {@link KeyspacePopulator} to execute during the bean initialization phase. The
	 * {@link KeyspacePopulator#populate(CqlSession) KeyspacePopulator} is invoked before creating
	 * {@link #setSchemaAction(SchemaAction) the schema}.
	 *
	 * @param keyspacePopulator the {@link KeyspacePopulator} to use during initialization.
	 * @see #setKeyspaceCleaner
	 */
	public void setKeyspacePopulator(@Nullable KeyspacePopulator keyspacePopulator) {
		this.keyspacePopulator = keyspacePopulator;
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
	 * Set the {@link CqlSession} to use.
	 *
	 * @param session must not be {@literal null}.
	 */
	public void setSession(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		this.session = session;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cassandra.config.CassandraCqlSessionFactoryBean#afterPropertiesSet()
	 */
	@Override
	@SuppressWarnings("all")
	public void afterPropertiesSet() throws Exception {

		Assert.state(this.session != null, "Session was not properly initialized");
		Assert.state(this.converter != null, "Converter was not properly initialized");

		super.afterPropertiesSet();

		if (this.keyspacePopulator != null) {
			this.keyspacePopulator.populate(getObject().getSession());
		}

		performSchemaAction();

		this.session.refreshSchema();
	}

	@Override
	protected SessionFactory createInstance() {
		return new DefaultSessionFactory(this.session);
	}

	@Override
	@SuppressWarnings("all")
	public void destroy() throws Exception {

		if (this.keyspaceCleaner != null) {
			this.keyspaceCleaner.populate(getObject().getSession());
		}
	}

	@Nullable
	@Override
	public Class<?> getObjectType() {
		return SessionFactory.class;
	}

	/**
	 * Perform the configure {@link SchemaAction} using {@link CassandraMappingContext} metadata.
	 */
	protected void performSchemaAction() throws Exception {

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
	 * Perform schema actions.
	 *
	 * @param drop {@literal true} to drop types/tables.
	 * @param dropUnused {@literal true} to drop unused types/tables (i.e. types/tables not know to be used by
	 *          {@link CassandraMappingContext}).
	 * @param ifNotExists {@literal true} to perform creations fail-safe by adding {@code IF NOT EXISTS} to each creation
	 *          statement.
	 */
	protected void createTables(boolean drop, boolean dropUnused, boolean ifNotExists) throws Exception {
		performSchemaActions(drop, dropUnused, ifNotExists);
	}

	@SuppressWarnings("all")
	private void performSchemaActions(boolean drop, boolean dropUnused, boolean ifNotExists) throws Exception {

		CassandraAdminOperations adminOperations = new CassandraAdminTemplate(getObject(), this.converter);

		CassandraPersistentEntitySchemaCreator schemaCreator =
				new CassandraPersistentEntitySchemaCreator(this.converter.getMappingContext(), adminOperations);

		if (drop) {

			CassandraPersistentEntitySchemaDropper schemaDropper =
					new CassandraPersistentEntitySchemaDropper(this.converter.getMappingContext(), adminOperations);

			schemaDropper.dropTables(dropUnused);
			schemaDropper.dropUserTypes(dropUnused);
		}

		schemaCreator.createUserTypes(ifNotExists);
		schemaCreator.createTables(ifNotExists);
		schemaCreator.createIndexes(ifNotExists);
	}
}
