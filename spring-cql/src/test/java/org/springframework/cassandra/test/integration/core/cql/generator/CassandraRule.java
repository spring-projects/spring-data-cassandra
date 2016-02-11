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

package org.springframework.cassandra.test.integration.core.cql.generator;

import java.util.ArrayList;
import java.util.List;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;
import org.springframework.cassandra.test.integration.support.SpringCqlBuildProperties;
import org.springframework.util.Assert;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Rule to provide a Cassandra context for integration tests. This rule can use/spin up either an embedded Cassandra
 * instance or use an external instance.
 * 
 * @author Mark Paluch
 */
public class CassandraRule extends ExternalResource {

	private final SpringCqlBuildProperties properties = new SpringCqlBuildProperties();
	private final String configurationFileName;
	private final long startUpTimeout;

	private List<CQLDataSet> before = new ArrayList<CQLDataSet>();
	private List<CQLDataSet> after = new ArrayList<CQLDataSet>();

	private Session session;
	private Cluster cluster;

	/**
	 * Creates a new {@link CassandraRule}.
	 */
	public CassandraRule() {
		this(null);
	}

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
	 * Add a {@link CQLDataSet} to execute before the test run.
	 * 
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule before(CQLDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CQLDataSet must not be null");
		before.add(cqlDataSet);

		return this;
	}

	/**
	 * Add a {@link CQLDataSet} to execute before the test run.
	 * 
	 * @param cqlDataSet must not be {@literal null}
	 * @return the rule
	 */
	public CassandraRule after(CQLDataSet cqlDataSet) {

		Assert.notNull(cqlDataSet, "CQLDataSet must not be null");
		after.add(cqlDataSet);

		return this;
	}

	@Override
	protected void before() throws Exception {

		if (properties.getCassandraType() == SpringCqlBuildProperties.CassandraType.EMBEDDED) {
			/* start an embedded Cassandra */
			if (configurationFileName != null) {
				EmbeddedCassandraServerHelper.startEmbeddedCassandra(configurationFileName, startUpTimeout);
			} else {
				EmbeddedCassandraServerHelper.startEmbeddedCassandra(startUpTimeout);
			}
		}
		/* create structure and load data */
		load();
	}

	/**
	 * Load the environment.
	 */
	protected void load() {

		String hostIp;
		int port;

		if (properties.getCassandraType() == SpringCqlBuildProperties.CassandraType.EMBEDDED) {
			hostIp = EmbeddedCassandraServerHelper.getHost();
			port = EmbeddedCassandraServerHelper.getNativeTransportPort();
		} else {
			hostIp = properties.getCassandraHost();
			port = properties.getCassandraPort();
		}

		cluster = new Cluster.Builder().addContactPoints(hostIp).withPort(port).build();
		session = cluster.connect();
		CQLDataLoader dataLoader = new CQLDataLoader(session);
		for (CQLDataSet cqlDataSet : before) {
			dataLoader.load(cqlDataSet);
		}

		session = dataLoader.getSession();
	}

	@Override
	protected void after() {

		super.after();

		CQLDataLoader dataLoader = new CQLDataLoader(session);
		for (CQLDataSet cqlDataSet : after) {
			dataLoader.load(cqlDataSet);
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
}
