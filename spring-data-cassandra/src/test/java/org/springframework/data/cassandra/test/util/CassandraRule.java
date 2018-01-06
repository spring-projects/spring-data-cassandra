/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.springframework.data.cassandra.test.util.CassandraRule.InvocationMode.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.rules.ExternalResource;
import org.springframework.data.cassandra.core.cql.SessionCallback;
import org.springframework.data.cassandra.support.CassandraConnectionProperties;
import org.springframework.data.cassandra.support.CqlDataSet;
import org.springframework.data.cassandra.support.IntegrationTestNettyOptions;
import org.springframework.util.Assert;
import org.springframework.util.SocketUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

/**
 * Rule to provide a Cassandra context for integration tests. This rule can use/spin up either an embedded Cassandra
 * instance or use an external instance. Typical usage:
 *
 * <pre>
 * {
 * 	public class MyIntegrationTest {
 * 		&#064;Rule public CassandraRule rule = new CassandraRule(CONFIG). //
 * 				before(new ClassPathCQLDataSet("CreateIndexCqlGeneratorIntegrationTests-BasicTest.cql", "keyspace"));
 * 	}
 * }
 * </pre>
 *
 * @author Mark Paluch
 * @since 1.5
 */
public class CassandraRule extends ExternalResource {

	private static ResourceHolder resourceHolder;

	private final CassandraConnectionProperties properties = new CassandraConnectionProperties();

	private final String configurationFileName;

	private final long startUpTimeout;

	private List<SessionCallback<Void>> before = new ArrayList<>();

	private Map<SessionCallback<?>, InvocationMode> invocationModeMap = new HashMap<>();

	private List<SessionCallback<Void>> after = new ArrayList<>();

	private Session session;

	private Cluster cluster;

	private CassandraRule parent;

	private Integer cassandraPort;

	/**
	 * Create a new {@link CassandraRule} and allows the use of a config file.
	 *
	 * @param yamlConfigurationResource name of the configuration resource, must not be {@literal null} and not empty
	 */
	public CassandraRule(String yamlConfigurationResource) {
		this(yamlConfigurationResource, EmbeddedCassandraServerHelper.DEFAULT_STARTUP_TIMEOUT_MS);
	}

	/**
	 * Create a new {@link CassandraRule}, allows the use of a config file and to provide a startup timeout.
	 *
	 * @param yamlConfigurationResource name of the configuration resource, must not be {@literal null} and not empty
	 * @param startUpTimeout the startup timeout
	 */
	public CassandraRule(String yamlConfigurationResource, long startUpTimeout) {

		Assert.hasText(yamlConfigurationResource, "Configuration file name must not be empty!");

		this.configurationFileName = yamlConfigurationResource;
		this.startUpTimeout = startUpTimeout;
	}

	/**
	 * Create a new {@link CassandraRule} using a parent {@link CassandraRule} to preserve cluster/connection facilities.
	 *
	 * @param parent the parent instance
	 */
	private CassandraRule(CassandraRule parent) {

		this.configurationFileName = null;
		this.startUpTimeout = -1;
		this.parent = parent;
	}

	/**
	 * Add a {@link CqlDataSet} to execute before each test run.
	 *
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule before(CqlDataSet cqlDataSet) {
		return before(each(), cqlDataSet);
	}

	/**
	 * Add a {@link CqlDataSet} to execute before the test run.
	 *
	 * @param invocationMode must not be {@literal null}
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule before(InvocationMode invocationMode, final CqlDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CQLDataSet must not be null");

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
	public CassandraRule before(final SessionCallback<?> sessionCallback) {

		Assert.notNull(sessionCallback, "SessionCallback must not be null");
		return before(each(), sessionCallback);
	}

	/**
	 * Add a {@link SessionCallback} to execute before the test run.
	 *
	 * @param invocationMode must not be {@literal null}
	 * @param sessionCallback must not be {@literal null}
	 * @return the rule
	 */
	@SuppressWarnings("unchecked")
	public CassandraRule before(InvocationMode invocationMode, final SessionCallback<?> sessionCallback) {

		Assert.notNull(sessionCallback, "SessionCallback must not be null");

		before.add((SessionCallback<Void>) sessionCallback);
		invocationModeMap.put(sessionCallback, invocationMode);
		return this;
	}

	/**
	 * Add a {@link CqlDataSet} to execute before the test run.
	 *
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule after(final CqlDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CQLDataSet must not be null");

		after.add(session -> {
			load(CassandraRule.this.session, cqlDataSet);
			return null;
		});

		return this;
	}

	/**
	 * Execute a {@link CqlDataSet}.
	 *
	 * @param cqlDataSet the CQL data set, must not be {@literal null}.
	 */
	public void execute(CqlDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CQLDataSet must not be null");
		load(session, cqlDataSet);
	}

	/**
	 * Execute the {@code before} sequence.
	 *
	 * @throws Exception
	 */
	@Override
	public void before() throws Exception {

		startCassandraIfNeeded();
		setupConnection();
		executeBeforeHooks();
	}

	/**
	 * Execute the {@code after} sequence.
	 */
	@Override
	protected void after() {

		super.after();
		executeAfterHooks();
		cleanupConnection();
	}

	/**
	 * Returns the {@link Cluster}.
	 *
	 * @return the Cluster
	 */
	public Cluster getCluster() {
		return cluster;
	}

	/**
	 * Returns the {@link Session}. The session state can be initialized and pointing to a keyspace other than
	 * {@code system}.
	 *
	 * @return the Session
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * Returns the Cassandra port.
	 *
	 * @return the Cassandra port
	 */
	public int getPort() {

		Assert.state(cassandraPort != null, "Cassandra port is not initialized");
		return cassandraPort;
	}

	/**
	 * Creates a {@link CassandraRule} to be used in a own scope. The derived {@link CassandraRule} shares the connection
	 * of this instance and starts with a fresh before/after configuration.
	 *
	 * @return a derived {@link CassandraRule} sharing the connection of this instance
	 */
	public CassandraRule testInstance() {
		return new CassandraRule(this);
	}

	private void startCassandraIfNeeded() throws Exception {

		if (parent == null && properties.getCassandraType() == CassandraConnectionProperties.CassandraType.EMBEDDED) {

			/* start an embedded Cassandra instance*/
			if (!System.getProperties().containsKey("com.sun.management.jmxremote.port")) {
				System.setProperty("com.sun.management.jmxremote.port", "" + SocketUtils.findAvailableTcpPort(1024));
			}

			if (configurationFileName != null) {
				EmbeddedCassandraServerHelper.startEmbeddedCassandra(configurationFileName, startUpTimeout);
			}
		}
	}

	private void executeBeforeHooks() {

		for (SessionCallback<Void> sessionCallback : before) {

			InvocationMode invocationMode = invocationModeMap.get(sessionCallback);
			if (invocationMode == never()) {
				continue;
			}

			if (invocationMode == firstTest()) {
				invocationModeMap.put(sessionCallback, never());
			}

			sessionCallback.doInSession(session);
		}
	}

	private void executeAfterHooks() {

		for (SessionCallback<Void> sessionCallback : after) {
			sessionCallback.doInSession(session);
		}
	}

	private void setupConnection() {

		if (parent == null) {
			String hostIp;
			int port;

			if (properties.getCassandraType() == CassandraConnectionProperties.CassandraType.EMBEDDED) {
				hostIp = EmbeddedCassandraServerHelper.getHost();
				port = EmbeddedCassandraServerHelper.getNativeTransportPort();
			} else {
				hostIp = properties.getCassandraHost();
				port = properties.getCassandraPort();
			}
			cassandraPort = port;

			QueryOptions queryOptions = new QueryOptions();
			queryOptions.setRefreshSchemaIntervalMillis(0);

			SocketOptions socketOptions = new SocketOptions();
			socketOptions.setConnectTimeoutMillis((int) TimeUnit.SECONDS.toMillis(15));
			socketOptions.setReadTimeoutMillis((int) TimeUnit.SECONDS.toMillis(15));

			if (resourceHolder == null) {

				cluster = new Cluster.Builder().addContactPoints(hostIp) //
						.withPort(port) //
						.withQueryOptions(queryOptions) //
						.withMaxSchemaAgreementWaitSeconds(3) //
						.withSocketOptions(socketOptions) //
						.withNettyOptions(IntegrationTestNettyOptions.INSTANCE) //
						.build();

				if (properties.getBoolean("build.cassandra.reuse-cluster")) {
					resourceHolder = new ResourceHolder(cluster, cluster.connect());
				}
			} else {
				cluster = resourceHolder.cluster;
			}

		} else {
			cluster = parent.cluster;
			cassandraPort = parent.cassandraPort;
		}

		if (parent != null) {
			session = parent.getSession();
		} else if (resourceHolder == null) {
			session = cluster.connect();
		} else {
			session = resourceHolder.session;
		}
	}

	private void cleanupConnection() {

		if (resourceHolder == null) {
			if (parent == null) {
				session.close();
				cluster.closeAsync();
				cluster = null;
			} else {
				session.closeAsync();
			}
		}

		session = null;
	}

	private void load(Session session, final CqlDataSet cqlDataSet) {

		if (cqlDataSet.getKeyspaceName() != null && !cqlDataSet.getKeyspaceName().equals(session.getLoggedKeyspace())) {
			session.execute(String.format("USE %s;", cqlDataSet.getKeyspaceName()));
		}

		cqlDataSet.getCqlStatements().forEach(session::execute);
	}

	/**
	 * Invocation mode for before calls.
	 */
	public static class InvocationMode {

		private static final InvocationMode once = new InvocationMode();
		private static final InvocationMode each = new InvocationMode();
		private static final InvocationMode never = new InvocationMode();

		/**
		 * Invocation mode to invoke an action once at before the first test.
		 *
		 * @return the {@code on first test} invocation mode
		 */
		public static InvocationMode firstTest() {
			return once;
		}

		/**
		 * Invocation mode to invoke an action on each run.
		 *
		 * @return the {@code on each test} invocation mode
		 */
		public static InvocationMode each() {
			return each;
		}

		/**
		 * Invocation mode to never invoke an action.
		 *
		 * @return the {@code never} invocation mode
		 */
		static InvocationMode never() {
			return never;
		}

		private InvocationMode() {

		}
	}

	private static class ResourceHolder {

		private Cluster cluster;
		private Session session;

		public ResourceHolder(final Cluster cluster, final Session session) {
			this.cluster = cluster;
			this.session = session;

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				session.close();
				cluster.close();
			}));
		}
	}
}
