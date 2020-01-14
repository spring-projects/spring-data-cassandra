/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.cassandra.test.util;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.rules.ExternalResource;

import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.support.CassandraConnectionProperties;
import org.springframework.data.cassandra.support.CqlDataSet;
import org.springframework.util.Assert;
import org.springframework.util.SocketUtils;
import org.springframework.util.StringUtils;

import org.testcontainers.containers.CassandraContainer;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

/**
 * JUnit Rule used to provide a Cassandra context for integration tests. This rule can use/spin up either an embedded
 * Cassandra instance, a TestContainer or use an external instance. Typical usage:
 *
 * <pre>
 * public class MyIntegrationTest {
 * 	&#064;Rule public CassandraRule rule = new CassandraRule(CONFIG)
 * 			.before(new ClassPathCQLDataSet("CreateIndexCqlGeneratorIntegrationTests-BasicTest.cql", "keyspace"));
 * }
 * </pre>
 *
 * Derives its configuration from {@code /config/cassandra-connection.properties} and {@link System#getenv(String)}
 * {@code CASSANDRA_VERSION} to configure the Cassandra version via Testcontainers.
 *
 * @author Mark Paluch
 * @author John Blum
 * @since 1.5
 * @see CassandraConnectionProperties
 */
public class CassandraRule extends ExternalResource {

	private static ResourceHolder resourceHolder;

	private static CassandraContainer<?> container;

	private final long startupTimeout;

	@SuppressWarnings("all") private final CassandraConnectionProperties properties = new CassandraConnectionProperties();

	private CassandraRule parent;

	private CqlSession system;

	private CqlSession session;

	private Integer cassandraPort;

	private final List<SessionCallback<Void>> after = new ArrayList<>();
	private final List<SessionCallback<Void>> before = new ArrayList<>();

	private final Map<SessionCallback<?>, InvocationMode> invocationModeMap = new HashMap<>();

	private CqlSessionBuilder sessionBuilder;

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
	 * Constructs a new instance of {@link CassandraRule} initialized with the given YAML configuration resource, thereby
	 * allowing the use of a configuration file and to provide a startup timeout.
	 *
	 * @param yamlConfigurationResource {@link String name} of the configuration resource; must not be {@literal null} or
	 *          empty.
	 * @param startupTimeout long value indicating the startup timeout in milliseconds.
	 */
	public CassandraRule(String yamlConfigurationResource, long startupTimeout) {

		Assert.hasText(yamlConfigurationResource, "YAML configuration resource must not be empty");

		this.configurationFilename = yamlConfigurationResource;
		this.startupTimeout = startupTimeout;
	}

	/**
	 * Constructs a new instance of {@link CassandraRule} using the provided (parent) {@link CassandraRule} to preserve
	 * cluster/connection context.
	 *
	 * @param parent the {@link CassandraRule parent} instance.
	 */
	private CassandraRule(CassandraRule parent) {

		this.configurationFilename = null;
		this.startupTimeout = -1;
		this.parent = parent;
	}

	/**
	 * Creates a {@link CassandraRule} to be used in an "owning" scope. The derived {@link CassandraRule} shares the
	 * connection of {@literal this} instance and starts with a fresh before/after configuration.
	 *
	 * @return a derived {@link CassandraRule} sharing the connection of {@literal this} instance.
	 * @see #CassandraRule(CassandraRule)
	 */
	public CassandraRule testInstance() {
		return new CassandraRule(this);
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
	 * Returns the system {@link CqlSession}.
	 *
	 * @return the Session
	 */
	public CqlSession getSystemSession() {
		return this.system;
	}

	/**
	 * Returns the {@link CqlSession}. The session state can be initialized and pointing to a keyspace other than
	 * {@code system}.
	 *
	 * @return the Session
	 */
	public CqlSession getSession() {
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

			if (isEmbedded()) {
				runEmbeddedCassandra();
			} else {
				runTestcontainerCassandra();
			}
		}
	}

	private boolean isStartNeeded() {
		return isParent() && (isEmbedded() || isTestcontainers());
	}

	private void configureRemoteJmxPort() {

		if (!System.getProperties().containsKey("com.sun.management.jmxremote.port")) {
			System.setProperty("com.sun.management.jmxremote.port", String.valueOf(SocketUtils.findAvailableTcpPort(1024)));
		}
	}

	private void runEmbeddedCassandra() throws Exception {

		if (this.configurationFilename != null) {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra(this.configurationFilename, this.startupTimeout);
		}
	}

	private void runTestcontainerCassandra() {

		if (container == null) {
			String cassandra_version = System.getenv("CASSANDRA_VERSION");
			if (StringUtils.hasText(cassandra_version)) {
				container = new CassandraContainer<>("cassandra:" + cassandra_version);
			} else {
				container = new CassandraContainer<>();
			}

			container.start();

			this.properties.setCassandraHost(container.getContainerIpAddress());
			this.properties.setCassandraPort(container.getFirstMappedPort());
			this.properties.update();
		}
	}

	private synchronized void initializeConnection() {

		if (isParent()) {

			this.cassandraPort = resolvePort();

			if (resourceHolder == null) {

				this.sessionBuilder = buildCluster(this.cassandraPort);

				if (isClusterReuseEnabled()) {
					resourceHolder = new ResourceHolder(this.sessionBuilder);
				}
			} else {
				this.sessionBuilder = resourceHolder.sessionBuilder;
			}
		} else {
			this.cassandraPort = this.parent.cassandraPort;
			this.sessionBuilder = this.parent.sessionBuilder;
		}

		this.system = resolveSystemSession();
		this.session = resolveSession();
	}

	private CqlSessionBuilder buildCluster(int port) {

		String host = resolveHost();

		return CqlSession.builder().addContactPoint(InetSocketAddress.createUnresolved(host, port))
				.withLocalDatacenter("datacenter1");
	}

	private String resolveHost() {

		if (isTestcontainers()) {
			return container.getContainerIpAddress();
		}

		return isEmbedded() ? EmbeddedCassandraServerHelper.getHost() : this.properties.getCassandraHost();
	}

	private int resolvePort() {

		if (isTestcontainers()) {
			return container.getFirstMappedPort();
		}

		return isEmbedded() ? EmbeddedCassandraServerHelper.getNativeTransportPort() : this.properties.getCassandraPort();
	}

	private CqlSession resolveSystemSession() {

		return isNotParent() ? this.parent.getSystemSession()
				: resourceHolder != null ? resourceHolder.system : this.sessionBuilder.build();
	}

	private CqlSession resolveSession() {

		return isNotParent() ? this.parent.getSession()
				: resourceHolder != null ? resourceHolder.toUse : this.sessionBuilder.build();
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

	private boolean isTestcontainers() {
		return CassandraConnectionProperties.CassandraType.TESTCONTAINERS.equals(this.properties.getCassandraType());
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

	private void load(CqlSession session, CqlDataSet cqlDataSet) {

		Optional.of(cqlDataSet.getKeyspaceName()).filter(StringUtils::hasText)
				.filter(
						keyspaceName -> !keyspaceName.equals(session.getKeyspace().map(CqlIdentifier::toString).orElse("system")))
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

		private final CqlSessionBuilder sessionBuilder;
		final CqlSession system;
		final CqlSession toUse;

		private ResourceHolder(CqlSessionBuilder sessionBuilder) {
			this.sessionBuilder = sessionBuilder;
			this.system = sessionBuilder.build();
			this.toUse = sessionBuilder.build();

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				toUse.close();
				system.close();
			}));
		}
	}
}
