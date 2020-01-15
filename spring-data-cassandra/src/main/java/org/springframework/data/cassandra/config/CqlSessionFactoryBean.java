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

package org.springframework.data.cassandra.config;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaCreator;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaDropper;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.CassandraExceptionTranslator;
import org.springframework.data.cassandra.core.cql.generator.AlterKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.AlterKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

/**
 * Factory for creating and configuring a Cassandra {@link CqlSession}, which is a thread-safe singleton. As such, it is
 * sufficient to have one {@link CqlSession} per application and keyspace.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 * @since 3.0
 */
public class CqlSessionFactoryBean
		implements FactoryBean<CqlSession>, InitializingBean, DisposableBean, PersistenceExceptionTranslator {

	public static final int DEFAULT_PORT = 9042;
	public static final String DEFAULT_CONTACT_POINTS = "localhost";

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private static final boolean DEFAULT_CREATE_IF_NOT_EXISTS = false;
	private static final boolean DEFAULT_DROP_TABLES = false;
	private static final boolean DEFAULT_DROP_UNUSED_TABLES = false;
	private static final CassandraExceptionTranslator EXCEPTION_TRANSLATOR = new CassandraExceptionTranslator();

	private @Nullable CqlSession systemSession;
	private @Nullable CqlSession session;

	private String contactPoints = DEFAULT_CONTACT_POINTS;
	private int port = DEFAULT_PORT;
	private @Nullable String password;
	private @Nullable String username;

	private @Nullable String keyspaceName;
	private @Nullable String localDatacenter;

	private @Nullable SessionBuilderConfigurer sessionSessionBuilderConfigurer;

	private List<KeyspaceActions> keyspaceActions = new ArrayList<>();
	private Set<KeyspaceActionSpecification> keyspaceSpecifications = new HashSet<>();

	private List<CreateKeyspaceSpecification> keyspaceCreations = new ArrayList<>();
	private List<AlterKeyspaceSpecification> keyspaceAlterations = new ArrayList<>();
	private List<DropKeyspaceSpecification> keyspaceDrops = new ArrayList<>();

	private List<String> keyspaceStartupScripts = new ArrayList<>();
	private List<String> keyspaceShutdownScripts = new ArrayList<>();

	private List<String> startupScripts = Collections.emptyList();
	private List<String> shutdownScripts = Collections.emptyList();

	private @Nullable CassandraConverter converter;

	private SchemaAction schemaAction = SchemaAction.NONE;

	/**
	 * Null-safe operation to determine whether the Cassandra {@link CqlSession} is connected or not.
	 *
	 * @return a boolean value indicating whether the Cassandra {@link CqlSession} is connected.
	 * @see Session#isClosed()
	 * @see #getObject()
	 */
	public boolean isConnected() {

		CqlSession session = getObject();

		return !(session == null || session.isClosed());
	}

	/**
	 * Set a comma-delimited string of the contact points (hosts) to connect to. Default is {@code localhost}; see
	 * {@link #DEFAULT_CONTACT_POINTS}.
	 *
	 * @param contactPoints the contact points used by the new cluster.
	 */
	public void setContactPoints(String contactPoints) {
		this.contactPoints = contactPoints;
	}

	/**
	 * Set the port for the contact points. Default is {@code 9042}, see {@link #DEFAULT_PORT}.
	 *
	 * @param port the port used by the new cluster.
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Set the username to use.
	 *
	 * @param username The username to set.
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Set the password to use.
	 *
	 * @param password The password to set.
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Sets the name of the Cassandra Keyspace to connect to. Passing {@literal null} will cause the Cassandra System
	 * Keyspace to be used.
	 *
	 * @param keyspaceName a String indicating the name of the Keyspace in which to connect.
	 * @see #getKeyspaceName()
	 */
	public void setKeyspaceName(@Nullable String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	/**
	 * Gets the name of the Cassandra Keyspace to connect to.
	 *
	 * @return the name of the Cassandra Keyspace to connect to as a String.
	 * @see #setKeyspaceName(String)
	 */
	@Nullable
	protected String getKeyspaceName() {
		return this.keyspaceName;
	}

	/**
	 * @return the {@link List} of {@link KeyspaceActions}.
	 */
	public List<KeyspaceActions> getKeyspaceActions() {
		return Collections.unmodifiableList(this.keyspaceActions);
	}

	/**
	 * Set a {@link List} of {@link KeyspaceActions} to be executed on initialization. Keyspace actions may contain create
	 * and drop specifications.
	 *
	 * @param keyspaceActions the {@link List} of {@link KeyspaceActions}.
	 */
	public void setKeyspaceActions(List<KeyspaceActions> keyspaceActions) {
		this.keyspaceActions = new ArrayList<>(keyspaceActions);
	}

	/**
	 * @param keyspaceSpecifications The {@link KeyspaceActionSpecification} to set.
	 */
	public void setKeyspaceSpecifications(List<? extends KeyspaceActionSpecification> keyspaceSpecifications) {
		this.keyspaceSpecifications = new LinkedHashSet<>(keyspaceSpecifications);
	}

	/**
	 * Set a {@link List} of {@link CreateKeyspaceSpecification create keyspace specifications} that are executed when
	 * this factory is {@link #afterPropertiesSet() initialized}. {@link CreateKeyspaceSpecification Create keyspace
	 * specifications} are executed on a system session with no keyspace set, before executing
	 * {@link #setStartupScripts(List)}.
	 *
	 * @param specifications the {@link List} of {@link CreateKeyspaceSpecification create keyspace specifications}.
	 */
	public void setKeyspaceCreations(List<CreateKeyspaceSpecification> specifications) {
		this.keyspaceCreations = new ArrayList<>(specifications);
	}

	/**
	 * Set a {@link List} of {@link AlterKeyspaceSpecification alter keyspace specifications} that are executed when this
	 * factory is {@link #afterPropertiesSet() initialized}. {@link AlterKeyspaceSpecification Alter keyspace
	 * specifications} are executed on a system session with no keyspace set, before executing
	 * {@link #setStartupScripts(List)}.
	 *
	 * @param specifications the {@link List} of {@link CreateKeyspaceSpecification create keyspace specifications}.
	 */
	public void setKeyspaceAlterations(List<AlterKeyspaceSpecification> specifications) {
		this.keyspaceAlterations = new ArrayList<>(specifications);
	}

	/**
	 * Set a {@link List} of {@link DropKeyspaceSpecification drop keyspace specifications} that are executed when this
	 * factory is {@link #destroy() destroyed}. {@link DropKeyspaceSpecification Drop keyspace specifications} are
	 * executed on a system session with no keyspace set, before executing {@link #setShutdownScripts(List)}.
	 *
	 * @param specifications the {@link List} of {@link DropKeyspaceSpecification drop keyspace specifications}.
	 */
	public void setKeyspaceDrops(List<DropKeyspaceSpecification> specifications) {
		this.keyspaceDrops = new ArrayList<>(specifications);
	}

	/**
	 * Set a {@link List} of raw {@link String CQL statements} that are executed in the scope of the system keyspace when
	 * this factory is {@link #afterPropertiesSet() initialized}. Scripts are executed on a system session with no
	 * keyspace set, after executing {@link #setKeyspaceCreations(List)}.
	 *
	 * @param scripts the scripts to execute on startup
	 */
	public void setKeyspaceStartupScripts(List<String> scripts) {
		this.keyspaceStartupScripts = new ArrayList<>(scripts);
	}

	/**
	 * Set a {@link List} of raw {@link String CQL statements} that are executed in the scope of the system keyspace when
	 * this factory is {@link #destroy() destroyed}. {@link DropKeyspaceSpecification Drop keyspace specifications} are
	 * executed on a system session with no keyspace set, after executing {@link #setKeyspaceDrops(List)}.
	 *
	 * @param scripts the scripts to execute on shutdown
	 */
	public void setKeyspaceShutdownScripts(List<String> scripts) {
		this.keyspaceShutdownScripts = new ArrayList<>(scripts);
	}

	/**
	 * @return the {@link KeyspaceActionSpecification} associated with this factory.
	 */
	public Set<KeyspaceActionSpecification> getKeyspaceSpecifications() {
		return Collections.unmodifiableSet(this.keyspaceSpecifications);
	}

	/**
	 * Sets the name of the local datacenter.
	 *
	 * @param localDatacenter a String indicating the name of the local datacenter.
	 */
	public void setLocalDatacenter(@Nullable String localDatacenter) {
		this.localDatacenter = localDatacenter;
	}

	/**
	 * Returns a reference to the connected Cassandra {@link CqlSession}.
	 *
	 * @return a reference to the connected Cassandra {@link CqlSession}.
	 * @throws IllegalStateException if the Cassandra {@link CqlSession} was not properly initialized.
	 * @see Session
	 */
	protected CqlSession getSession() {

		CqlSession session = getObject();

		Assert.state(session != null, "Session was not properly initialized");

		return session;
	}

	/**
	 * Sets the {@link SessionBuilderConfigurer} to configure the
	 * {@link com.datastax.oss.driver.api.core.session.SessionBuilder}.
	 *
	 * @param sessionSessionBuilderConfigurer
	 */
	public void setSessionSessionBuilderConfigurer(@Nullable SessionBuilderConfigurer sessionSessionBuilderConfigurer) {
		this.sessionSessionBuilderConfigurer = sessionSessionBuilderConfigurer;
	}

	/**
	 * Sets CQL scripts to be executed immediately after the session is connected.
	 *
	 * @deprecated Use {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer} or
	 *             {@link SessionFactoryFactoryBean} with
	 *             {@link org.springframework.data.cassandra.core.cql.session.init.KeyspacePopulator} instead.
	 */
	@Deprecated
	public void setStartupScripts(@Nullable List<String> scripts) {
		this.startupScripts = (scripts != null ? new ArrayList<>(scripts) : Collections.emptyList());
	}

	/**
	 * Returns an unmodifiable list of startup scripts.
	 *
	 * @deprecated Use {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer} or
	 *             {@link SessionFactoryFactoryBean} with
	 *             {@link org.springframework.data.cassandra.core.cql.session.init.KeyspacePopulator} instead.
	 */
	@Deprecated
	public List<String> getStartupScripts() {
		return Collections.unmodifiableList(this.startupScripts);
	}

	/**
	 * Sets CQL scripts to be executed immediately before the session is shutdown.
	 *
	 * @deprecated Use {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer} or
	 *             {@link SessionFactoryFactoryBean} with
	 *             {@link org.springframework.data.cassandra.core.cql.session.init.KeyspacePopulator} instead.
	 */
	@Deprecated
	public void setShutdownScripts(@Nullable List<String> scripts) {
		this.shutdownScripts = scripts != null ? new ArrayList<>(scripts) : Collections.emptyList();
	}

	/**
	 * Returns an unmodifiable list of shutdown scripts.
	 *
	 * @deprecated Use {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer} or
	 *             {@link SessionFactoryFactoryBean} with
	 *             {@link org.springframework.data.cassandra.core.cql.session.init.KeyspacePopulator} instead.
	 */
	@Deprecated
	public List<String> getShutdownScripts() {
		return Collections.unmodifiableList(this.shutdownScripts);
	}

	/**
	 * Set the {@link CassandraConverter} to use. Schema actions will derive table and user type information from the
	 * {@link CassandraMappingContext} inside {@code converter}.
	 *
	 * @param converter must not be {@literal null}.
	 * @deprecated Use {@link CassandraSessionFactoryBean} with
	 *             {@link CassandraSessionFactoryBean#setConverter(CassandraConverter)} instead.
	 */
	@Deprecated
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
	 * @deprecated Use {@link CassandraSessionFactoryBean} with
	 *             {@link CassandraSessionFactoryBean#setSchemaAction(SchemaAction)} instead.
	 */
	@Deprecated
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		CqlSessionBuilder sessionBuilder = buildBuilder();
		this.systemSession = buildSystemSession(sessionBuilder);

		initializeCluster(this.systemSession);

		this.session = buildSession(sessionBuilder);

		executeScripts(getStartupScripts().stream(), this.session);
		performSchemaAction();
		this.systemSession.refreshSchema();
		this.session.refreshSchema();
	}

	/**
	 * Build the system session.
	 *
	 * @param sessionBuilder
	 * @return
	 */
	protected CqlSession buildSystemSession(CqlSessionBuilder sessionBuilder) {
		return sessionBuilder.withKeyspace("system").build();
	}

	/**
	 * Build the keyspace session.
	 *
	 * @param sessionBuilder
	 * @return
	 */
	protected CqlSession buildSession(CqlSessionBuilder sessionBuilder) {
		if (StringUtils.hasText(getKeyspaceName())) {
			sessionBuilder.withKeyspace(getKeyspaceName());
		}

		return sessionBuilder.build();
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() {

		if (session != null) {

			executeScripts(getShutdownScripts().stream(), this.session);

			executeSpecsAndScripts(keyspaceDrops, keyspaceShutdownScripts, this.systemSession);
			closeSystemSession();
			closeSession();
		}
	}

	/**
	 * Close the regular session object.
	 */
	protected void closeSession() {
		session.close();
	}

	/**
	 * Close the system session object.
	 */
	protected void closeSystemSession() {
		systemSession.close();
	}

	protected CqlSessionBuilder buildBuilder() {

		Assert.hasText(this.contactPoints, "At least one server is required");

		CqlSessionBuilder builder = CqlSession.builder();
		StringUtils.commaDelimitedListToSet(this.contactPoints).stream().forEach(host -> {
			builder.addContactPoint(InetSocketAddress.createUnresolved(host, this.port));
		});

		if (StringUtils.hasText(this.username)) {
			builder.withAuthCredentials(this.username, this.password);
		}

		if (StringUtils.hasText(this.localDatacenter)) {
			builder.withLocalDatacenter(this.localDatacenter);
		}

		if (this.sessionSessionBuilderConfigurer != null) {
			return this.sessionSessionBuilderConfigurer.configure(builder);
		}

		return builder;
	}

	private void initializeCluster(CqlSession session) {

		generateSpecificationsFromFactoryDeclarations();

		List<KeyspaceActionSpecification> startupSpecifications = new ArrayList<>(
				this.keyspaceCreations.size() + this.keyspaceAlterations.size());

		startupSpecifications.addAll(this.keyspaceCreations);
		startupSpecifications.addAll(this.keyspaceAlterations);

		executeSpecsAndScripts(startupSpecifications, this.keyspaceStartupScripts, session);
	}

	private void executeSpecsAndScripts(List<? extends KeyspaceActionSpecification> keyspaceActionSpecifications,
			List<String> scripts, CqlSession session) {

		if (!CollectionUtils.isEmpty(keyspaceActionSpecifications) || !CollectionUtils.isEmpty(scripts)) {

			Stream<String> keyspaceActions = keyspaceActionSpecifications.stream().map(this::toCql);

			executeScripts(Stream.concat(keyspaceActions, scripts.stream()), session);
		}
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
	 * Perform schema actions.
	 *
	 * @param drop {@literal true} to drop types/tables.
	 * @param dropUnused {@literal true} to drop unused types/tables (i.e. types/tables not know to be used by
	 *          {@link CassandraMappingContext}).
	 * @param ifNotExists {@literal true} to perform creations fail-safe by adding {@code IF NOT EXISTS} to each creation
	 *          statement.
	 */
	protected void createTables(boolean drop, boolean dropUnused, boolean ifNotExists) {

		CassandraAdminTemplate adminTemplate = new CassandraAdminTemplate(this.session, converter);
		performSchemaActions(drop, dropUnused, ifNotExists, adminTemplate);
	}

	private void performSchemaActions(boolean drop, boolean dropUnused, boolean ifNotExists,
			CassandraAdminOperations adminOperations) {

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(
				getMappingContext(), adminOperations);

		if (drop) {

			CassandraPersistentEntitySchemaDropper schemaDropper = new CassandraPersistentEntitySchemaDropper(
					getMappingContext(), adminOperations);

			schemaDropper.dropTables(dropUnused);
			schemaDropper.dropUserTypes(dropUnused);
		}

		schemaCreator.createUserTypes(ifNotExists);
		schemaCreator.createTables(ifNotExists);
		schemaCreator.createIndexes(ifNotExists);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public CqlSession getObject() {
		return this.session;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<? extends CqlSession> getObjectType() {
		return CqlSession.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * Executes the given Cassandra CQL scripts. The {@link CqlSession} must be connected when this method is called.
	 */
	private void executeScripts(Stream<String> scripts, CqlSession session) {

		scripts.forEach(script -> {
			logger.info("executing raw CQL [{}]", script);
			session.execute(script);
		});
	}

	/**
	 * Evaluates the contents of all the KeyspaceSpecificationFactoryBean and generates the proper KeyspaceSpecification
	 * from them.
	 */
	private void generateSpecificationsFromFactoryDeclarations() {

		generateSpecifications(this.keyspaceSpecifications);
		this.keyspaceActions.forEach(actions -> generateSpecifications(actions.getActions()));
	}

	private void generateSpecifications(Collection<KeyspaceActionSpecification> specifications) {

		specifications.forEach(keyspaceActionSpecification -> {

			if (keyspaceActionSpecification instanceof AlterKeyspaceSpecification) {
				this.keyspaceAlterations.add((AlterKeyspaceSpecification) keyspaceActionSpecification);
			} else if (keyspaceActionSpecification instanceof CreateKeyspaceSpecification) {
				this.keyspaceCreations.add((CreateKeyspaceSpecification) keyspaceActionSpecification);
			} else if (keyspaceActionSpecification instanceof DropKeyspaceSpecification) {
				this.keyspaceDrops.add((DropKeyspaceSpecification) keyspaceActionSpecification);
			}
		});
	}

	private String toCql(KeyspaceActionSpecification specification) {

		if (specification instanceof AlterKeyspaceSpecification) {
			return new AlterKeyspaceCqlGenerator((AlterKeyspaceSpecification) specification).toCql();
		} else if (specification instanceof CreateKeyspaceSpecification) {
			return new CreateKeyspaceCqlGenerator((CreateKeyspaceSpecification) specification).toCql();
		} else if (specification instanceof DropKeyspaceSpecification) {
			return new DropKeyspaceCqlGenerator((DropKeyspaceSpecification) specification).toCql();
		}

		throw new IllegalArgumentException(
				"Unsupported specification type: " + ClassUtils.getQualifiedName(specification.getClass()));
	}

	@Nullable
	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException e) {
		return EXCEPTION_TRANSLATOR.translateExceptionIfPossible(e);
	}
}
