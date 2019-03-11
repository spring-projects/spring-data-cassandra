/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.cassandra.test.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.rules.ExternalResource;

import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.support.CassandraConnectionProperties;
import org.springframework.data.cassandra.support.CqlDataSet;
import org.springframework.data.cassandra.support.IntegrationTestNettyOptions;
import org.springframework.util.Assert;
import org.springframework.util.SocketUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

/**
 * JUnit Rule used to provide a Cassandra context for integration tests.
 *
 * This rule can use/spin up either an embedded Cassandra instance or use an external instance.
 *
 * Typical usage:
 *
 * <pre>
 * public class MyIntegrationTest {
 * 		&#064;Rule public CassandraRule rule = new CassandraRule(CONFIG)
 * 				.before(new ClassPathCQLDataSet("CreateIndexCqlGeneratorIntegrationTests-BasicTest.cql", "keyspace"));
 * }
 * </pre>
 *
 * @author Mark Paluch
 * @author John Blum
 * @since 1.5
 */
public class CassandraRule extends ExternalResource {

	private static ResourceHolder resourceHolder;

	private final long startupTimeout;

	@SuppressWarnings("all")
	private final CassandraConnectionProperties properties = new CassandraConnectionProperties();

	private CassandraRule parent;

	private Cluster cluster;

	private Integer cassandraPort;

	private final List<SessionCallback<Void>> after = new ArrayList<>();
	private final List<SessionCallback<Void>> before = new ArrayList<>();

	private final Map<SessionCallback<?>, InvocationMode> invocationModeMap = new HashMap<>();

	private Session session;

	private final String configurationFilename;

	/**
	 * Create a new {@link CassandraRule} and allows the use of a config file.
	 *
	 * @param yamlConfigurationResource name of the configuration resource, must not be {@literal null} and not empty
	 */
	public CassandraRule(String yamlConfigurationResource) {
		this(yamlConfigurationResource, EmbeddedCassandraServerHelper.DEFAULT_STARTUP_TIMEOUT_MS);
	}

	/**
	 * Constructs a new instance of {@link CassandraRule} initialized with the given YAML configuration resource,
	 * thereby allowing the use of a configuration file and to provide a startup timeout.
	 *
	 * @param yamlConfigurationResource {@link String name} of the configuration resource;
	 * must not be {@literal null} or empty.
	 * @param startupTimeout long value indicating the startup timeout in milliseconds.
	 */
	public CassandraRule(String yamlConfigurationResource, long startupTimeout) {

		Assert.hasText(yamlConfigurationResource, "YAML configuration resource must not be empty");

		this.configurationFilename = yamlConfigurationResource;
		this.startupTimeout = startupTimeout;
	}

	/**
	 * Constructs a new instance of {@link CassandraRule} using the provided (parent) {@link CassandraRule}
	 * to preserve cluster/connection context.
	 *
	 * @param parent the {@link CassandraRule parent} instance.
	 */
	private CassandraRule(CassandraRule parent) {

		this.configurationFilename = null;
		this.startupTimeout = -1;
		this.parent = parent;
	}

	/**
	 * Creates a {@link CassandraRule} to be used in an "owning" scope.
	 *
	 * The derived {@link CassandraRule} shares the connection of {@literal this} instance
	 * and starts with a fresh before/after configuration.
	 *
	 * @return a derived {@link CassandraRule} sharing the connection of {@literal this} instance.
	 * @see #CassandraRule(CassandraRule)
	 */
	public CassandraRule testInstance() {
		return new CassandraRule(this);
	}

	/**
	 * Returns the {@link Cluster}.
	 *
	 * @return the Cluster
	 */
	public Cluster getCluster() {
		return this.cluster;
	}

	/**
	 * Returns the Cassandra port.
	 *
	 * @return the Cassandra port
	 */
	public int getPort() {

		Assert.state(this.cassandraPort != null, "Cassandra port was not initialized");

		return this.cassandraPort;
	}

	/**
	 * Returns the {@link Session}. The session state can be initialized and pointing to a keyspace other than
	 * {@code system}.
	 *
	 * @return the Session
	 */
	public Session getSession() {
		return this.session;
	}

	/**
	 * Add a {@link CqlDataSet} to execute before each test run.
	 *
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule before(CqlDataSet cqlDataSet) {
		return before(InvocationMode.EACH, cqlDataSet);
	}

	/**
	 * Add a {@link CqlDataSet} to execute before the test run.
	 *
	 * @param invocationMode must not be {@literal null}
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule before(InvocationMode invocationMode, CqlDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CqlDataSet must not be null");

		SessionCallback<Void> sessionCallback = session -> {
			load(session, cqlDataSet);
			return null;
		};

		before(invocationMode, sessionCallback);

		return this;
	}

	/**
	 * Add a {@link SessionCallback} to execute before each test run.
	 *
	 * @param sessionCallback must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule before(SessionCallback<?> sessionCallback) {
		return before(InvocationMode.EACH, sessionCallback);
	}

	/**
	 * Add a {@link SessionCallback} to execute before the test run.
	 *
	 * @param invocationMode must not be {@literal null}
	 * @param sessionCallback must not be {@literal null}
	 * @return the rule
	 */
	@SuppressWarnings("unchecked")
	public CassandraRule before(InvocationMode invocationMode, SessionCallback<?> sessionCallback) {

		Assert.notNull(sessionCallback, "SessionCallback must not be null");

		this.before.add((SessionCallback<Void>) sessionCallback);
		this.invocationModeMap.put(sessionCallback, invocationMode);

		return this;
	}

	/**
	 * Add a {@link CqlDataSet} to execute before the test run.
	 *
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	@SuppressWarnings("unused")
	public CassandraRule after(CqlDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CqlDataSet must not be null");

		this.after.add(session -> {
			load(CassandraRule.this.session, cqlDataSet);
			return null;
		});

		return this;
	}

	/**
	 * Execute {@code before} sequence.
	 */
	@Override
	protected void before() throws Exception {

		startCassandraIfNeeded();
		initializeConnection();
		executeBeforeHooks();
	}

	private void startCassandraIfNeeded() throws Exception {

		if (isStartNeeded()) {
			configureRemoteJmxPort();
			runEmbeddedCassandra();
		}
	}

	private boolean isStartNeeded() {
		return isParent() && isEmbedded();
	}

	private void configureRemoteJmxPort() {

		if (!System.getProperties().containsKey("com.sun.management.jmxremote.port")) {
			System.setProperty("com.sun.management.jmxremote.port",
				String.valueOf(SocketUtils.findAvailableTcpPort(1024)));
		}
	}

	private void runEmbeddedCassandra() throws Exception {

		if (this.configurationFilename != null) {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra(this.configurationFilename, this.startupTimeout);
		}
	}

	private synchronized void initializeConnection() {

		if (isParent()) {

			this.cassandraPort = resolvePort();

			if (resourceHolder == null) {

				this.cluster = buildCluster(this.cassandraPort);

				if (isClusterReuseEnabled()) {
					resourceHolder = new ResourceHolder(this.cluster);
				}
			} else {
				this.cluster = resourceHolder.cluster;
			}
		} else {
			this.cassandraPort = this.parent.cassandraPort;
			this.cluster = this.parent.cluster;
		}

		this.session = resolveSession();
	}

	private Cluster buildCluster(int port) {

		QueryOptions queryOptions = new QueryOptions().setRefreshSchemaIntervalMillis(0);

		SocketOptions socketOptions = new SocketOptions()
			.setConnectTimeoutMillis((int) TimeUnit.SECONDS.toMillis(15))
			.setReadTimeoutMillis((int) TimeUnit.SECONDS.toMillis(15));

		String host = resolveHost();

		return new Cluster.Builder()
				.addContactPoints(host)
				.withPort(port)
				.withMaxSchemaAgreementWaitSeconds(3)
				.withNettyOptions(IntegrationTestNettyOptions.INSTANCE)
				.withQueryOptions(queryOptions)
				.withSocketOptions(socketOptions)
				.build();
	}

	private String resolveHost() {

		return isEmbedded()
				? EmbeddedCassandraServerHelper.getHost()
				: this.properties.getCassandraHost();
	}

	private int resolvePort() {

		return isEmbedded()
				? EmbeddedCassandraServerHelper.getNativeTransportPort()
				: this.properties.getCassandraPort();
	}

	private Session resolveSession() {

		return isNotParent() ? this.parent.getSession()
			: resourceHolder != null ? resourceHolder.session
			: this.cluster.connect();
	}

	private void executeBeforeHooks() {

		this.before.forEach(sessionCallback -> {

			InvocationMode invocationMode = this.invocationModeMap.get(sessionCallback);

			if (!InvocationMode.NEVER.equals(invocationMode)) {

				if (InvocationMode.ONCE.equals(invocationMode)) {
					this.invocationModeMap.put(sessionCallback, InvocationMode.NEVER);
				}

				sessionCallback.doInSession(this.session);
			}
		});
	}

	/**
	 * Execute the {@code after} sequence.
	 */
	@Override
	protected void after() {

		super.after();
		executeAfterHooks();
		releaseConnection();
	}

	private void executeAfterHooks() {
		this.after.forEach(sessionCallback -> sessionCallback.doInSession(this.session));
	}

	private synchronized void releaseConnection() {

		if (resourceHolder == null) {
			if (isParent()) {
				this.session.close();
				this.cluster.closeAsync();
				this.cluster = null;
			} else {
				this.session.closeAsync();
			}
		}

		this.session = null;
	}

	private boolean isClusterReuseEnabled() {
		return this.properties.getBoolean("build.cassandra.reuse-cluster");
	}

	private boolean isEmbedded() {
		return CassandraConnectionProperties.CassandraType.EMBEDDED.equals(this.properties.getCassandraType());
	}

	private boolean isNotParent() {
		return !isParent();
	}

	private boolean isParent() {
		return this.parent == null;
	}

	/**
	 * Execute a {@link CqlDataSet}.
	 *
	 * @param cqlDataSet the CQL data set, must not be {@literal null}.
	 */
	public void execute(CqlDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CqlDataSet must not be null");

		load(this.session, cqlDataSet);
	}

	private void load(Session session, CqlDataSet cqlDataSet) {

		Optional.of(cqlDataSet.getKeyspaceName())
			.filter(StringUtils::hasText)
			.filter(keyspaceName -> !keyspaceName.equals(session.getLoggedKeyspace()))
			.ifPresent(keyspaceName -> session.execute(String.format(KeyspaceRule.USE_KEYSPACE_CQL, keyspaceName)));

		cqlDataSet.getCqlStatements().forEach(session::execute);
	}

	/**
	 * Invocation mode for before calls.
	 */
	public enum InvocationMode {

		/** {@link InvocationMode} to invoke an action before each test. */
		EACH,

		/** {@link InvocationMode} to never invoke an action. */
		NEVER,

		/** {@link InvocationMode} to invoke an action once before the first test. */
		ONCE

	}

	private static class ResourceHolder {

		private Cluster cluster;
		private Session session;

		private ResourceHolder(Cluster cluster) {
			this(cluster, cluster.connect());
		}

		private ResourceHolder(Cluster cluster, Session session) {

			this.cluster = cluster;
			this.session = session;

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				session.close();
				cluster.close();
			}));
		}
	}
}
