/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceActionSpecification;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Convenient factory for configuring a Cassandra Cluster.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David Webb
 */
public class CassandraCqlClusterFactoryBean implements FactoryBean<Cluster>, InitializingBean, DisposableBean,
		PersistenceExceptionTranslator {

	public static final String DEFAULT_CONTACT_POINTS = "localhost";
	public static final boolean DEFAULT_METRICS_ENABLED = true;
	public static final boolean DEFAULT_DEFERRED_INITIALIZATION = false;
	public static final boolean DEFAULT_JMX_REPORTING_ENABLED = true;
	public static final boolean DEFAULT_SSL_ENABLED = false;
	public static final int DEFAULT_PORT = 9042;

	protected static final Logger log = LoggerFactory.getLogger(CassandraCqlClusterFactoryBean.class);

	private Cluster cluster;

	/*
	 * Attributes needed for cluster builder
	 */
	private String contactPoints = DEFAULT_CONTACT_POINTS;
	private int port = CassandraCqlClusterFactoryBean.DEFAULT_PORT;
	private CompressionType compressionType;
	private PoolingOptions poolingOptions;
	private SocketOptions socketOptions;
	private AuthProvider authProvider;
	private String username;
	private String password;
	private LoadBalancingPolicy loadBalancingPolicy;
	private ReconnectionPolicy reconnectionPolicy;
	private RetryPolicy retryPolicy;
	private boolean metricsEnabled = DEFAULT_METRICS_ENABLED;
	private boolean jmxReportingEnabled = DEFAULT_JMX_REPORTING_ENABLED;
	private boolean sslEnabled = DEFAULT_SSL_ENABLED;
	private SSLOptions sslOptions;
	private Host.StateListener hostStateListener;
	private LatencyTracker latencyTracker;
	private Set<KeyspaceActionSpecification<?>> keyspaceSpecifications = new HashSet<KeyspaceActionSpecification<?>>();
	private List<CreateKeyspaceSpecification> keyspaceCreations = new ArrayList<CreateKeyspaceSpecification>();
	private List<DropKeyspaceSpecification> keyspaceDrops = new ArrayList<DropKeyspaceSpecification>();
	private List<String> startupScripts = new ArrayList<String>();
	private List<String> shutdownScripts = new ArrayList<String>();

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	@Override
	public Cluster getObject() throws Exception {
		return cluster;
	}

	@Override
	public Class<? extends Cluster> getObjectType() {
		return Cluster.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		if (!StringUtils.hasText(contactPoints)) {
			throw new IllegalArgumentException("at least one server is required");
		}

		Cluster.Builder builder = Cluster.builder();

		builder.addContactPoints(StringUtils.commaDelimitedListToStringArray(contactPoints)).withPort(port);

		if (compressionType != null) {
			builder.withCompression(convertCompressionType(compressionType));
		}

		if (poolingOptions != null) {
			builder.withPoolingOptions(poolingOptions);
		}

		if (socketOptions != null) {
			builder.withSocketOptions(socketOptions);
		}

		if (authProvider != null) {
			builder.withAuthProvider(authProvider);

			if (username != null) {
				builder.withCredentials(username, password);
			}
		}

		if (loadBalancingPolicy != null) {
			builder.withLoadBalancingPolicy(loadBalancingPolicy);
		}

		if (reconnectionPolicy != null) {
			builder.withReconnectionPolicy(reconnectionPolicy);
		}

		if (retryPolicy != null) {
			builder.withRetryPolicy(retryPolicy);
		}

		if (!metricsEnabled) {
			builder.withoutMetrics();
		}

		if (!jmxReportingEnabled) {
			builder.withoutJMXReporting();
		}

		if (sslEnabled) {
			if (sslOptions == null) {
				builder.withSSL();
			} else {
				builder.withSSL(sslOptions);
			}
		}

		cluster = builder.build();

		if (hostStateListener != null) {
			cluster.register(hostStateListener);
		}

		if (latencyTracker != null) {
			cluster.register(latencyTracker);
		}

		generateSpecificationsFromFactoryBeans();

		executeSpecsAndScripts(keyspaceCreations, startupScripts);
	}

	/**
	 * Examines the contents of all the KeyspaceSpecificationFactoryBeans and generates the proper KeyspaceSpecification
	 * from them.
	 */
	private void generateSpecificationsFromFactoryBeans() {

		for (KeyspaceActionSpecification<?> spec : keyspaceSpecifications) {

			if (spec instanceof CreateKeyspaceSpecification) {
				keyspaceCreations.add((CreateKeyspaceSpecification) spec);
			}
			if (spec instanceof DropKeyspaceSpecification) {
				keyspaceDrops.add((DropKeyspaceSpecification) spec);
			}

		}

	}

	protected void executeSpecsAndScripts(@SuppressWarnings("rawtypes") List specs, List<String> scripts) {

		Session system = null;
		CqlTemplate template = null;

		try {
			if (specs != null) {
				system = specs.size() == 0 ? null : cluster.connect();
				template = system == null ? null : new CqlTemplate(system);

				Iterator<?> i = specs.iterator();
				while (i.hasNext()) {
					KeyspaceActionSpecification<?> spec = (KeyspaceActionSpecification<?>) i.next();
					String cql = (spec instanceof CreateKeyspaceSpecification) ? new CreateKeyspaceCqlGenerator(
							(CreateKeyspaceSpecification) spec).toCql() : new DropKeyspaceCqlGenerator(
							(DropKeyspaceSpecification) spec).toCql();

					template.execute(cql);
				}
			}

			if (scripts != null) {

				if (system == null) {
					system = scripts.size() == 0 ? null : cluster.connect();
				}

				if (template == null) {
					template = system == null ? null : new CqlTemplate(system);
				}

				for (String script : scripts) {

					if (log.isDebugEnabled()) {
						log.debug("executing raw CQL [{}]", script);
					}

					template.execute(script);
				}
			}
		} finally {

			if (system != null) {
				system.close();
			}
		}
	}

	@Override
	public void destroy() throws Exception {

		executeSpecsAndScripts(keyspaceDrops, shutdownScripts);
		cluster.close();
	}

	/**
	 * Sets a comma-delimited string of the contact points (hosts) to connect to.
	 */
	public void setContactPoints(String contactPoints) {
		this.contactPoints = contactPoints;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setCompressionType(CompressionType compressionType) {
		this.compressionType = compressionType;
	}

	public void setPoolingOptions(PoolingOptions poolingOptions) {
		this.poolingOptions = poolingOptions;
	}

	public void setSocketOptions(SocketOptions socketOptions) {
		this.socketOptions = socketOptions;
	}

	public void setAuthProvider(AuthProvider authProvider) {
		this.authProvider = authProvider;
	}

	public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
		this.loadBalancingPolicy = loadBalancingPolicy;
	}

	public void setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
		this.reconnectionPolicy = reconnectionPolicy;
	}

	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	public void setMetricsEnabled(boolean metricsEnabled) {
		this.metricsEnabled = metricsEnabled;
	}

	public void setKeyspaceCreations(List<CreateKeyspaceSpecification> specifications) {
		this.keyspaceCreations = specifications;
	}

	public List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return keyspaceCreations;
	}

	public void setKeyspaceDrops(List<DropKeyspaceSpecification> specifications) {
		this.keyspaceDrops = specifications;
	}

	public List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return keyspaceDrops;
	}

	public void setStartupScripts(List<String> scripts) {
		this.startupScripts = scripts;
	}

	public void setShutdownScripts(List<String> scripts) {
		this.shutdownScripts = scripts;
	}

	private static Compression convertCompressionType(CompressionType type) {
		switch (type) {
			case NONE:
				return Compression.NONE;
			case SNAPPY:
				return Compression.SNAPPY;
			case LZ4:
				return Compression.LZ4;
		}
		throw new IllegalArgumentException("unknown compression type " + type);
	}

	/**
	 * @return Returns the keyspaceSpecifications.
	 */
	public Set<KeyspaceActionSpecification<?>> getKeyspaceSpecifications() {
		return keyspaceSpecifications;
	}

	/**
	 * If accumlating is true, we append to the list, otherwise we replace the list.
	 * 
	 * @param keyspaceSpecifications The keyspaceSpecifications to set.
	 */
	public void setKeyspaceSpecifications(Set<KeyspaceActionSpecification<?>> keyspaceSpecifications) {
		this.keyspaceSpecifications = keyspaceSpecifications;
	}

	/**
	 * @param username The username to set.
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @param password The password to set.
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @param jmxReportingEnabled The jmxReportingEnabled to set.
	 */
	public void setJmxReportingEnabled(boolean jmxReportingEnabled) {
		this.jmxReportingEnabled = jmxReportingEnabled;
	}

	/**
	 * @param sslEnabled The sslEnabled to set.
	 */
	public void setSslEnabled(boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}

	/**
	 * @param sslOptions The sslOptions to set.
	 */
	public void setSslOptions(SSLOptions sslOptions) {
		this.sslOptions = sslOptions;
	}

	/**
	 * @param hostStateListener The hostStateListener to set.
	 */
	public void setHostStateListener(Host.StateListener hostStateListener) {
		this.hostStateListener = hostStateListener;
	}

	/**
	 * @param latencyTracker The latencyTracker to set.
	 */
	public void setLatencyTracker(LatencyTracker latencyTracker) {
		this.latencyTracker = latencyTracker;
	}
}
