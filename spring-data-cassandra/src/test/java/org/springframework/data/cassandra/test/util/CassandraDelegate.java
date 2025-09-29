/*
 * Copyright 2017-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.support.CassandraConnectionProperties;
import org.springframework.data.cassandra.support.CqlDataSet;
import org.springframework.data.util.Optionals;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;

/**
 * Delegate used to provide a Cassandra context for integration tests. This rule can use/spin up either a TestContainer
 * or use an external instance. Derives its configuration from {@code /config/cassandra-connection.properties} to
 * configure the Cassandra version via Testcontainers.
 *
 * @author Mark Paluch
 * @author John Blum
 * @author Tomasz Lelek
 * @see org.springframework.data.cassandra.support.CassandraConnectionProperties
 * @see com.datastax.oss.driver.api.core.CqlSessionBuilder
 * @see com.datastax.oss.driver.api.core.CqlSession
 * @since 1.5
 */
class CassandraDelegate {

	private static CassandraContainer container;

	private static final Logger log = LoggerFactory.getLogger(CassandraDelegate.class);

	private static ResourceHolder resourceHolder;

	private final CassandraConnectionProperties properties = new CassandraConnectionProperties();

	private CqlSession session;
	private CqlSession system;

	private CqlSessionBuilder sessionBuilder;

	private @Nullable String cassandraHost;
	private @Nullable Integer cassandraPort;

	private final List<SessionCallback<Void>> after = new ArrayList<>();
	private final List<SessionCallback<Void>> before = new ArrayList<>();

	private final Map<SessionCallback<?>, InvocationMode> invocationModeMap = new HashMap<>();

	/**
	 * Returns the Cassandra host.
	 *
	 * @return the Cassandra host
	 */
	public String getHost() {

		Assert.state(this.cassandraHost != null, "Cassandra host was not initialized");

		return this.cassandraHost;
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
	public CassandraDelegate before(CqlDataSet cqlDataSet) {
		return before(InvocationMode.EACH, cqlDataSet);
	}

	/**
	 * Add a {@link CqlDataSet} to execute before the test run.
	 *
	 * @param invocationMode must not be {@literal null}
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	private CassandraDelegate before(InvocationMode invocationMode, CqlDataSet cqlDataSet) {

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
	public CassandraDelegate before(SessionCallback<?> sessionCallback) {
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
	private CassandraDelegate before(InvocationMode invocationMode, SessionCallback<?> sessionCallback) {

		Assert.notNull(sessionCallback, "SessionCallback must not be null");

		this.before.add((SessionCallback<Void>) sessionCallback);
		this.invocationModeMap.put(sessionCallback, invocationMode);

		return this;
	}

	/**
	 * Execute the {@code after} sequence.
	 */
	void after() {

		executeAfterHooks();
		releaseConnection();
	}

	/**
	 * Add a {@link CqlDataSet} to execute before the test run.
	 *
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	@SuppressWarnings("unused")
	public CassandraDelegate after(CqlDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CqlDataSet must not be null");

		this.after.add(session -> {
			load(CassandraDelegate.this.session, cqlDataSet);
			return null;
		});

		return this;
	}

	/**
	 * Execute {@code before} sequence.
	 */
	public void before() throws Exception {

		startCassandraIfNeeded();
		initializeConnection();
		executeBeforeHooks();
	}

	private void startCassandraIfNeeded() throws Exception {

		if (isStartNeeded()) {
			startCassandraContainer();
		}
	}

	private boolean isStartNeeded() {
		return isTestcontainers();
	}

	private void startCassandraContainer() {

		if (container == null) {

			container = getCassandraDockerImageName().map(CassandraContainer::new)
					.orElseGet(() -> new CassandraContainer("cassandra:5.0.3")).withReuse(true);

			container.withEnv("MAX_HEAP_SIZE", "1500M");
			container.withEnv("HEAP_NEWSIZE", "300M");

			DockerImageName imageName = DockerImageName.parse(container.getDockerImageName());
			String versionPart = imageName.getVersionPart();
			Version version = Version.parse(versionPart);

			if (version.getMajor() >= 5) {
				container.withCopyToContainer(MountableFile.forClasspathResource("testcontainer-cassandra-v5.yaml"),
						"/etc/cassandra/cassandra.yaml");
			}
			container.start();

			log.info("Running with Cassandra Docker Testcontainer Image Name [{}] at [{}]", container.getDockerImageName(),
					container.getMappedPort(9042));

			this.properties.setCassandraHost(container.getContactPoint().getHostString());
			this.properties.setCassandraPort(container.getMappedPort(9042));
			this.properties.update();
		}
	}

	private Optional<String> getCassandraDockerImageName() {

		return resolveCassandraVersion().map(cassandraVersion -> String.format("cassandra:%s", cassandraVersion));
	}

	private Optional<String> resolveCassandraVersion() {

		return Optionals.firstNonEmpty(() -> Optional.ofNullable(System.getenv("CASSANDRA_VERSION")),
				() -> Optional.ofNullable(System.getProperty("cassandra.version"))).filter(StringUtils::hasText);
	}

	private synchronized void initializeConnection() {

		this.cassandraHost = resolveHost();
		this.cassandraPort = resolvePort();

		if (resourceHolder == null) {

			this.sessionBuilder = buildCluster(this.cassandraPort);

			if (isClusterReuseEnabled()) {
				resourceHolder = new ResourceHolder(this.sessionBuilder);
			}
		} else {
			this.sessionBuilder = resourceHolder.sessionBuilder;
		}

		this.system = resolveSystemSession();
		this.session = resolveSession();
	}

	private CqlSessionBuilder buildCluster(int port) {

		String host = resolveHost();

		CqlSessionBuilder builder = CqlSession.builder().addContactPoint(InetSocketAddress.createUnresolved(host, port))
				.withLocalDatacenter("datacenter1");

		Version cassandraVersion = getCassandraVersion(builder);

		if (cassandraVersion.getMajor() >= 4) {
			return builder.withConfigLoader(
					DriverConfigLoader.programmaticBuilder().withString(DefaultDriverOption.PROTOCOL_VERSION, "V5").build());
		} else {
			return builder;
		}
	}

	private Version getCassandraVersion(CqlSessionBuilder builder) {

		try (CqlSession cqlSession = builder.build()) {

			return cqlSession.getMetadata().getNodes() //
					.values() //
					.stream() //
					.map(Node::getCassandraVersion) //
					.findFirst() //
					.orElseThrow(() -> new IllegalStateException("Cannot determine Cassandra version"));
		}
	}

	private String resolveHost() {

		if (isTestcontainers()) {
			return container.getContainerIpAddress();
		}

		return this.properties.getCassandraHost();
	}

	private int resolvePort() {

		if (isTestcontainers()) {
			return container.getMappedPort(9042);
		}

		return this.properties.getCassandraPort();
	}

	private CqlSession resolveSystemSession() {
		return resourceHolder != null ? resourceHolder.system : this.sessionBuilder.build();
	}

	private CqlSession resolveSession() {
		return resourceHolder != null ? resourceHolder.toUse : this.sessionBuilder.build();
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

	private void executeAfterHooks() {
		this.after.forEach(sessionCallback -> sessionCallback.doInSession(this.session));
	}

	private synchronized void releaseConnection() {

		if (resourceHolder == null && this.session != null) {
			this.session.close();
		}

		this.session = null;
	}

	private boolean isClusterReuseEnabled() {
		return this.properties.getBoolean("build.cassandra.reuse-cluster");
	}

	private boolean isTestcontainers() {
		return CassandraConnectionProperties.CassandraType.TESTCONTAINERS.equals(this.properties.getCassandraType());
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
				.ifPresent(keyspaceName -> session.execute(String.format(TestKeyspaceDelegate.USE_KEYSPACE_CQL, keyspaceName)));

		cqlDataSet.getCqlStatements().forEach(session::execute);
	}

	/**
	 * Create a {@link CqlSession} object.
	 *
	 * @return a new {@link CqlSession}.
	 * @see com.datastax.oss.driver.api.core.CqlSession
	 */
	CqlSession createSession() {
		return sessionBuilder.build();
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
		private final CqlSession system;
		private final CqlSession toUse;

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
