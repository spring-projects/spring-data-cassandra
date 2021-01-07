/*
 * Copyright 2013-2021 the original author or authors.
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
import java.util.function.IntFunction;
import java.util.stream.Collectors;
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
 * @author Tomasz Lelek
 * @since 3.0
 */
public class CqlSessionFactoryBean
		implements FactoryBean<CqlSession>, InitializingBean, DisposableBean, PersistenceExceptionTranslator {

	public static final String CASSANDRA_SYSTEM_SESSION = "system";
	public static final String DEFAULT_CONTACT_POINTS = "localhost";
	public static final int DEFAULT_PORT = 9042;

	private static final boolean DEFAULT_CREATE_IF_NOT_EXISTS = false;
	private static final boolean DEFAULT_DROP_TABLES = false;
	private static final boolean DEFAULT_DROP_UNUSED_TABLES = false;

	private static final CassandraExceptionTranslator EXCEPTION_TRANSLATOR = new CassandraExceptionTranslator();

	private int port = DEFAULT_PORT;

	private @Nullable CassandraConverter converter;

	private @Nullable CqlSession session;
	private @Nullable CqlSession systemSession;

	private List<KeyspaceActions> keyspaceActions = new ArrayList<>();

	private List<AlterKeyspaceSpecification> keyspaceAlterations = new ArrayList<>();
	private List<CreateKeyspaceSpecification> keyspaceCreations = new ArrayList<>();
	private List<DropKeyspaceSpecification> keyspaceDrops = new ArrayList<>();

	private List<String> keyspaceStartupScripts = new ArrayList<>();
	private List<String> keyspaceShutdownScripts = new ArrayList<>();

	private List<String> startupScripts = Collections.emptyList();
	private List<String> shutdownScripts = Collections.emptyList();

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private Set<KeyspaceActionSpecification> keyspaceSpecifications = new HashSet<>();

	private SchemaAction schemaAction = SchemaAction.NONE;

	private @Nullable SessionBuilderConfigurer sessionBuilderConfigurer;

	private IntFunction<Collection<InetSocketAddress>> contactPoints = port -> createInetSocketAddresses(
			DEFAULT_CONTACT_POINTS, port);

	private @Nullable String keyspaceName;
	private @Nullable String localDatacenter;
	private @Nullable String password;
	private @Nullable String username;

	/**
	 * Null-safe operation to determine whether the Cassandra {@link CqlSession} is connected or not.
	 *
	 * @return a boolean value indicating whether the Cassandra {@link CqlSession} is connected.
	 * @see com.datastax.oss.driver.api.core.session.Session#isClosed()
	 * @see #getObject()
	 */
	public boolean isConnected() {

		CqlSession session = getObject();

		return !(session == null || session.isClosed());
	}

	/**
	 * Set a comma-delimited string of the contact points (hosts) to connect to. Default is {@code localhost}; see
	 * {@link #DEFAULT_CONTACT_POINTS}. Contact points may use the form {@code host:port}, or a simple {@code host} to use
	 * the configured {@link #setPort(int) port}.
	 *
	 * @param contactPoints the contact points used by the new cluster, must not be {@literal null}.
	 */
	public void setContactPoints(String contactPoints) {

		Assert.hasText(contactPoints, "Contact points must not be empty");

		this.contactPoints = port -> createInetSocketAddresses(contactPoints, port);
	}

	/**
	 * Set a collection of the contact points (hosts) to connect to. Default is {@code localhost}; see
	 * {@link #DEFAULT_CONTACT_POINTS}.
	 *
	 * @param contactPoints the contact points used by the new cluster, must not be {@literal null}. Use
	 *          {@link InetSocketAddress#createUnresolved(String, int) unresolved addresses} to delegate hostname
	 *          resolution to the driver.
	 * @since 3.1
	 */
	public void setContactPoints(Collection<InetSocketAddress> contactPoints) {

		Assert.notNull(contactPoints, "Contact points must not be null");

		this.contactPoints = unusedPort -> contactPoints;
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
	 * Set the {@link CassandraConverter} to use. Schema actions will derive table and user type information from the
	 * {@link CassandraMappingContext} inside {@code converter}.
	 *
	 * @param converter must not be {@literal null}.
	 * @deprecated Use {@link SessionFactoryFactoryBean} with
	 *             {@link SessionFactoryFactoryBean#setConverter(CassandraConverter)} instead.
	 */
	@Deprecated
	public void setConverter(CassandraConverter converter) {

		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
	}

	/**
	 * @return the configured {@link CassandraConverter}.
	 */
	@Nullable
	public CassandraConverter getConverter() {
		return this.converter;
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
	 * @return the {@link List} of {@link KeyspaceActions}.
	 */
	public List<KeyspaceActions> getKeyspaceActions() {
		return Collections.unmodifiableList(this.keyspaceActions);
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
	 * @param keyspaceSpecifications The {@link KeyspaceActionSpecification} to set.
	 */
	public void setKeyspaceSpecifications(List<? extends KeyspaceActionSpecification> keyspaceSpecifications) {
		this.keyspaceSpecifications = new LinkedHashSet<>(keyspaceSpecifications);
	}

	/**
	 * @return the {@link KeyspaceActionSpecification} associated with this factory.
	 */
	public Set<KeyspaceActionSpecification> getKeyspaceSpecifications() {
		return Collections.unmodifiableSet(this.keyspaceSpecifications);
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

	/**
	 * Returns a reference to the connected Cassandra {@link CqlSession}.
	 *
	 * @return a reference to the connected Cassandra {@link CqlSession}.
	 * @throws IllegalStateException if the Cassandra {@link CqlSession} was not properly initialized.
	 * @see com.datastax.oss.driver.api.core.CqlSession
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
	 * @param sessionBuilderConfigurer
	 */
	public void setSessionBuilderConfigurer(@Nullable SessionBuilderConfigurer sessionBuilderConfigurer) {
		this.sessionBuilderConfigurer = sessionBuilderConfigurer;
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

		executeCql(getStartupScripts().stream(), this.session);
		performSchemaAction();

		this.systemSession.refreshSchema();
		this.session.refreshSchema();
	}

	protected CqlSessionBuilder buildBuilder() {

		Collection<InetSocketAddress> addresses = contactPoints.apply(this.port);
		Assert.notEmpty(addresses, "At least one server is required");

		CqlSessionBuilder sessionBuilder = createBuilder();

		addresses.forEach(sessionBuilder::addContactPoint);

		if (StringUtils.hasText(this.username)) {
			sessionBuilder.withAuthCredentials(this.username, this.password);
		}

		if (StringUtils.hasText(this.localDatacenter)) {
			sessionBuilder.withLocalDatacenter(this.localDatacenter);
		}

		return this.sessionBuilderConfigurer != null ? this.sessionBuilderConfigurer.configure(sessionBuilder)
				: sessionBuilder;
	}

	CqlSessionBuilder createBuilder() {
		return CqlSession.builder();
	}

	/**
	 * Build the Cassandra {@link CqlSession System Session}.
	 *
	 * @param sessionBuilder {@link CqlSessionBuilder} used to a build a Cassandra {@link CqlSession}.
	 * @return the built Cassandra {@link CqlSession System Session}.
	 * @see com.datastax.oss.driver.api.core.CqlSessionBuilder
	 * @see com.datastax.oss.driver.api.core.CqlSession
	 */
	protected CqlSession buildSystemSession(CqlSessionBuilder sessionBuilder) {
		return sessionBuilder.withKeyspace(CASSANDRA_SYSTEM_SESSION).build();
	}

	/**
	 * Build a {@link CqlSession Session} to the user-defined {@literal Keyspace} or the default {@literal Keyspace} if
	 * the user did not specify a {@literal Keyspace} by {@link String name}.
	 *
	 * @param sessionBuilder {@link CqlSessionBuilder} used to a build a Cassandra {@link CqlSession}.
	 * @return the built {@link CqlSession} to the user-defined {@literal Keyspace}.
	 * @see com.datastax.oss.driver.api.core.CqlSessionBuilder
	 * @see com.datastax.oss.driver.api.core.CqlSession
	 */
	protected CqlSession buildSession(CqlSessionBuilder sessionBuilder) {

		if (StringUtils.hasText(getKeyspaceName())) {
			sessionBuilder.withKeyspace(getKeyspaceName());
		}

		return sessionBuilder.build();
	}

	private void initializeCluster(CqlSession session) {

		generateSpecificationsFromFactoryBeanDeclarations();

		List<KeyspaceActionSpecification> keyspaceStartupSpecifications = new ArrayList<>(
				this.keyspaceCreations.size() + this.keyspaceAlterations.size());

		keyspaceStartupSpecifications.addAll(this.keyspaceCreations);
		keyspaceStartupSpecifications.addAll(this.keyspaceAlterations);

		executeSpecificationsAndScripts(keyspaceStartupSpecifications, this.keyspaceStartupScripts, session);
	}

	/**
	 * Evaluates the contents of all the {@link KeyspaceActionSpecificationFactoryBean}s and generates the proper
	 * {@link KeyspaceActionSpecification}s from them.
	 */
	private void generateSpecificationsFromFactoryBeanDeclarations() {

		generateSpecifications(this.keyspaceSpecifications);
		this.keyspaceActions.forEach(actions -> generateSpecifications(actions.getActions()));
	}

	private void generateSpecifications(Collection<KeyspaceActionSpecification> specifications) {

		specifications.forEach(specification -> {

			if (specification instanceof AlterKeyspaceSpecification) {
				this.keyspaceAlterations.add((AlterKeyspaceSpecification) specification);
			} else if (specification instanceof CreateKeyspaceSpecification) {
				this.keyspaceCreations.add((CreateKeyspaceSpecification) specification);
			} else if (specification instanceof DropKeyspaceSpecification) {
				this.keyspaceDrops.add((DropKeyspaceSpecification) specification);
			}
		});
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
	 * @param dropUnused {@literal true} to drop unused types/tables (i.e. types/tables not known to be used by the
	 *          {@link CassandraMappingContext}).
	 * @param ifNotExists {@literal true} to perform fail-safe creations by adding {@code IF NOT EXISTS} to each creation
	 *          statement.
	 */
	protected void createTables(boolean drop, boolean dropUnused, boolean ifNotExists) {

		CassandraAdminTemplate adminTemplate = new CassandraAdminTemplate(this.session, this.converter);

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
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	@Nullable
	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException e) {
		return EXCEPTION_TRANSLATOR.translateExceptionIfPossible(e);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() {

		if (this.session != null) {
			executeCql(getShutdownScripts().stream(), this.session);
			executeSpecificationsAndScripts(this.keyspaceDrops, this.keyspaceShutdownScripts, this.systemSession);
			closeSession();
			closeSystemSession();
		}
	}

	/**
	 * Close the regular session object.
	 */
	protected void closeSession() {
		this.session.close();
	}

	/**
	 * Close the system session object.
	 */
	protected void closeSystemSession() {
		this.systemSession.close();
	}

	/**
	 * Executes the given, raw Cassandra CQL scripts. The {@link CqlSession} must be connected when this method is called.
	 *
	 * @see com.datastax.oss.driver.api.core.CqlSession#execute(String)
	 */
	private void executeCql(Stream<String> cql, CqlSession session) {

		cql.forEach(query -> {
			this.logger.info("Executing CQL [{}]", query);
			session.execute(query);
		});
	}

	private void executeSpecificationsAndScripts(List<? extends KeyspaceActionSpecification> keyspaceActionSpecifications,
			List<String> keyspaceCqlScripts, CqlSession session) {

		if (!CollectionUtils.isEmpty(keyspaceActionSpecifications) || !CollectionUtils.isEmpty(keyspaceCqlScripts)) {

			Stream<String> keyspaceActionSpecificationsStream = keyspaceActionSpecifications.stream()
					.map(CqlSessionFactoryBean::toCql);
			Stream<String> keyspaceCqlScriptsStream = keyspaceCqlScripts.stream();
			Stream<String> cql = Stream.concat(keyspaceActionSpecificationsStream, keyspaceCqlScriptsStream);

			executeCql(cql, session);
		}
	}

	/**
	 * Converts the {@link KeyspaceActionSpecification} to {@link String CQL}.
	 *
	 * @param specification {@link KeyspaceActionSpecification} to convert to {@link String CQL}.
	 * @return a {@link String} containing the CQL for the given {@link KeyspaceActionSpecification}.
	 * @see org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification
	 */
	private static String toCql(KeyspaceActionSpecification specification) {

		if (specification instanceof AlterKeyspaceSpecification) {
			return new AlterKeyspaceCqlGenerator((AlterKeyspaceSpecification) specification).toCql();
		} else if (specification instanceof CreateKeyspaceSpecification) {
			return new CreateKeyspaceCqlGenerator((CreateKeyspaceSpecification) specification).toCql();
		} else if (specification instanceof DropKeyspaceSpecification) {
			return new DropKeyspaceCqlGenerator((DropKeyspaceSpecification) specification).toCql();
		}

		throw new IllegalArgumentException(
				String.format("Unsupported specification type: %s", ClassUtils.getQualifiedName(specification.getClass())));
	}

	private static Collection<InetSocketAddress> createInetSocketAddresses(String contactPoints, int defaultPort) {

		return StringUtils.commaDelimitedListToSet(contactPoints) //
				.stream() //
				.map(contactPoint -> HostAndPort.createWithDefaultPort(contactPoint, defaultPort)) //
				.map(hostAndPort -> InetSocketAddress.createUnresolved(hostAndPort.getHost(), hostAndPort.getPort())) //
				.collect(Collectors.toList());
	}

	/**
	 * Value object to encapsulate host and port.
	 */
	private static class HostAndPort {

		private final String host;

		private final int port;

		private HostAndPort(String host, int port) {
			this.host = host;
			this.port = port;
		}

		/**
		 * Create a {@link HostAndPort} from a contact point. Contact points may contain a port or can be specified
		 * port-less. Contact points may be:
		 * <ul>
		 * <li>Plain IPv4 addresses ({@code 1.2.3.4})</li>
		 * <li>Hostnames ({@code foo.bar.baz})</li>
		 * <li>IPv6 without brackets {@code 1:2::3}</li>
		 * <li>IPv6 with brackets {@code [1:2::3]}</li>
		 * <li>IPv4 addresses with port ({@code 1.2.3.4:1234})</li>
		 * <li>Hostnames with port ({@code foo.bar.baz:1234})</li>
		 * <li>IPv6 with brackets and port {@code [1:2::3]:1234}</li>
		 * </ul>
		 *
		 * @param contactPoint must not be {@literal null}.
		 * @param defaultPort
		 * @return the host and port representation.
		 */
		static HostAndPort createWithDefaultPort(String contactPoint, int defaultPort) {

			int i = contactPoint.lastIndexOf(':');

			if (i == -1 || !isValidPort(contactPoint.substring(i + 1))) {
				return new HostAndPort(contactPoint, defaultPort);
			}

			String[] hostAndPort = contactPoint.split(":");
			String host;
			int port = defaultPort;

			if (hostAndPort.length != 2) {

				int bracketEnd = contactPoint.indexOf(']');
				if (contactPoint.startsWith("[") && bracketEnd != -1) {

					// IPv6 as resource identifier enclosed with brackets [ ]
					host = contactPoint.substring(0, bracketEnd + 1);
					String remainder = contactPoint.substring(bracketEnd + 1);

					if (remainder.startsWith(":")) {
						port = Integer.parseInt(remainder.substring(1));
					}
				} else {
					// everything else
					host = contactPoint;
				}
			} else {
				host = hostAndPort[0];
				port = Integer.parseInt(hostAndPort[1]);
			}
			return new HostAndPort(host, port);
		}

		private static boolean isValidPort(String value) {
			try {
				int i = Integer.parseInt(value);
				return i > 0 && i < 65535;
			} catch (NumberFormatException ex) {
				return false;
			}
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}
	}
}
