/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cassandra.test.integration;

import static org.springframework.cassandra.test.integration.CassandraRule.InvocationMode.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.rules.ExternalResource;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.test.integration.support.CassandraConnectionProperties;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;
import org.springframework.util.SocketUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Rule to provide a Cassandra context for integration tests. This rule can use/spin up either an embedded Cassandra
 * instance or use an external instance.
 *
 * @author Mark Paluch
 */
public class CassandraRule extends ExternalResource {

	private final CassandraConnectionProperties properties = new CassandraConnectionProperties();
	private final String configurationFileName;
	private final long startUpTimeout;

	private List<SessionCallback<Void>> before = new ArrayList<SessionCallback<Void>>();
	private Map<SessionCallback<?>, InvocationMode> invocationModeMap = new HashMap<SessionCallback<?>, InvocationMode>();
	private List<SessionCallback<Void>> after = new ArrayList<SessionCallback<Void>>();

	private Session session;
	private Cluster cluster;
	private CassandraRule parent;

	/**
	 * Creates a new {@link CassandraRule} and allows the use of a config file.
	 *
	 * @param configurationFileName
	 */
	public CassandraRule(String configurationFileName) {
		this(configurationFileName, EmbeddedCassandraServerHelper.DEFAULT_STARTUP_TIMEOUT);
	}

	/**
	 * Creates a new {@link CassandraRule}, allows the use of a config file and to provide a startup timeout.
	 *
	 * @param configurationFileName
	 * @param startUpTimeout
	 */
	public CassandraRule(String configurationFileName, long startUpTimeout) {

		Assert.hasText(configurationFileName, "Configuration file name must not be empty!");

		this.configurationFileName = configurationFileName;
		this.startUpTimeout = startUpTimeout;
	}

	/**
	 * Creates a new {@link CassandraRule} using a parent {@link CassandraRule} to preserve cluster/connection facilities.
	 *
	 * @param parent
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

		SessionCallback<Void> sessionCallback = new SessionCallback<Void>() {
			@Override
			public Void doInSession(Session s) throws DataAccessException {
				load(s, cqlDataSet);
				return null;
			}
		};

		before(invocationMode, sessionCallback);
		return this;
	}

	public void execute(CqlDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CQLDataSet must not be null");
		load(session, cqlDataSet);
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

		after.add(new SessionCallback<Void>() {
			@Override
			public Void doInSession(Session s) throws DataAccessException {
				load(session, cqlDataSet);
				return null;
			}
		});

		return this;
	}

	@Override
	public void before() throws Exception {

		startCassandraIfNeeded();
		setupConnection();
		executeBeforeHooks();
	}

	@Override
	protected void after() {

		super.after();
		executeAfterHooks();
		cleanupConnection();
	}

	/**
	 * Returns the {@link Cluster}.
	 *
	 * @return
	 */
	public Cluster getCluster() {
		return cluster;
	}

	/**
	 * Returns the {@link Session}. The session state can be initialized and pointing to a keyspace other than
	 * {@code system}.
	 *
	 * @return
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * Creates a {@link CassandraRule} for each test instance. Derived
	 *
	 * @return
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

	private void executeAfterHooks() {

		for (SessionCallback<Void> sessionCallback : after) {
			sessionCallback.doInSession(session);
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

			cluster = new Cluster.Builder().addContactPoints(hostIp).withPort(port).build();
		} else {
			cluster = parent.cluster;
		}

		session = cluster.connect();
	}

	private void cleanupConnection() {

		if (parent == null) {
			session.close();
			cluster.closeAsync();
			cluster = null;
		} else {
			session.closeAsync();
		}

		session = null;
	}

	private void load(Session session, final CqlDataSet cqlDataSet) {


		if(cqlDataSet.getKeyspaceName() != null && !cqlDataSet.getKeyspaceName().equals(session.getLoggedKeyspace())){
			session.execute(String.format("USE %s;", cqlDataSet.getKeyspaceName()));
		}

		for (String statement : cqlDataSet.getCqlStatements()) {
			session.execute(statement);
		}
	}

	/**
	 * Invocation mode for before calls.
	 */
	public static class InvocationMode {

		private final static InvocationMode once = new InvocationMode();
		private final static InvocationMode each = new InvocationMode();
		private final static InvocationMode never = new InvocationMode();

		/**
		 * Invocation mode to invoke an action once at before the first test.
		 *
		 * @return
		 */
		public static InvocationMode firstTest() {
			return once;
		}

		/**
		 * Invocation mode to invoke an action on each run.
		 *
		 * @return
		 */
		public static InvocationMode each() {
			return each;
		}

		/**
		 * Invocation mode to never invoke an action.
		 *
		 * @return
		 */
		static InvocationMode never() {
			return never;
		}

		private InvocationMode() {

		}
	}
}
