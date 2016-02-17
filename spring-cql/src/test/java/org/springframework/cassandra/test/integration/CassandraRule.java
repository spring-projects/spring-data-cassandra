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

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
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
		this.configurationFileName = configurationFileName;
		this.startUpTimeout = startUpTimeout;
	}


	/**
	 * Add a {@link CQLDataSet} to execute before each test run.
	 *
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule before(CQLDataSet cqlDataSet) {
		return before(each(), cqlDataSet);
	}

	/**
	 * Add a {@link CQLDataSet} to execute before the test run.
	 *
	 * @param invocationMode must not be {@literal null}
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule before(InvocationMode invocationMode, final CQLDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CQLDataSet must not be null");
		SessionCallback<Void> sessionCallback = new SessionCallback<Void>() {
			@Override
			public Void doInSession(Session s) throws DataAccessException {
				CQLDataLoader dataLoader = new CQLDataLoader(session);
				dataLoader.load(cqlDataSet);
				return null;
			}
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
	public CassandraRule before(InvocationMode invocationMode, final SessionCallback<?> sessionCallback) {

		Assert.notNull(sessionCallback, "SessionCallback must not be null");
		before.add((SessionCallback<Void>) sessionCallback);
		invocationModeMap.put(sessionCallback, invocationMode);
		return this;
	}

	/**
	 * Add a {@link CQLDataSet} to execute before the test run.
	 *
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule after(final CQLDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CQLDataSet must not be null");
		after.add(new SessionCallback<Void>() {
			@Override
			public Void doInSession(Session s) throws DataAccessException {
				CQLDataLoader dataLoader = new CQLDataLoader(session);
				dataLoader.load(cqlDataSet);
				return null;
			}
		});

		return this;
	}

	@Override
	public void before() throws Exception {
		startCassandraIfNeeded();

		/* create structure and load data */
		load();
	}

	void startCassandraIfNeeded() throws Exception {

		if (properties.getCassandraType() == CassandraConnectionProperties.CassandraType.EMBEDDED) {

			/* start an embedded Cassandra instance*/
			if (!System.getProperties().containsKey("com.sun.management.jmxremote.port")) {
				System.setProperty("com.sun.management.jmxremote.port", "" + SocketUtils.findAvailableTcpPort(1024));
			}

			if (configurationFileName != null) {
				EmbeddedCassandraServerHelper.startEmbeddedCassandra(configurationFileName, startUpTimeout);
			} else {
				EmbeddedCassandraServerHelper.startEmbeddedCassandra(startUpTimeout);
			}
		}
	}

	/**
	 * Load the environment.
	 */
	protected void load() {

		setupConnection();

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
		session = cluster.connect();
	}

	@Override
	protected void after() {

		super.after();

		for (SessionCallback<Void> sessionCallback : after) {
			sessionCallback.doInSession(session);
		}

		session.close();
		cluster.close();
		session = null;
		cluster = null;
	}

	public Session getSession() {
		return session;
	}

	public Cluster getCluster() {
		return cluster;
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
