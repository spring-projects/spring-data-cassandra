/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.core.cql.CassandraExceptionTranslator;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.generator.AlterKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.AlterKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;

/**
 * {@link org.springframework.beans.factory.FactoryBean} for configuring a Cassandra {@link Cluster}.
 * <p>
 * This factory bean allows configuration and creation of {@link Cluster} bean. Most options default to {@literal null}.
 * Unsupported options are configured via {@link ClusterBuilderConfigurer}.
 * <p/>
 * The factory bean initializes keyspaces, if configured, accoording to its lifecycle. Keyspaces can be created after
 * {@link #afterPropertiesSet() initialization} and dropped when this factory is {@link #destroy() destroyed}. Keyspace
 * actions can be configured via {@link #setKeyspaceActions(List) XML} and {@link #setKeyspaceCreations(List)
 * programatically}. Additional {@link #getStartupScripts()} and {@link #getShutdownScripts()} are executed after
 * running keyspace actions.
 * <p/>
 * <strong>XML configuration</strong>
 *
 * <pre class="code">
     <cql:cluster contact-points="…"
				  port="${build.cassandra.native_transport_port}" compression="SNAPPY" netty-options-ref="nettyOptions">
		<cql:local-pooling-options
				min-simultaneous-requests="26" max-simultaneous-requests="101"
				core-connections="3" max-connections="9"/>
		<cql:remote-pooling-options
				min-simultaneous-requests="25" max-simultaneous-requests="100"
				core-connections="1" max-connections="2"/>
		<cql:socket-options connect-timeout-millis="5000"
							 keep-alive="true" reuse-address="true" so-linger="60" tcp-no-delay="true"
							 receive-buffer-size="65536" send-buffer-size="65536"/>
		<cql:keyspace name="${cassandra.keyspace}" action="CREATE_DROP"
					   durable-writes="true"/>
	</cass:cluster>
 * </pre>
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David Webb
 * @author Kirk Clemens
 * @author Jorge Davison
 * @author John Blum
 * @author Mark Paluch
 * @author Stefan Birkner
 * @see org.springframework.beans.factory.InitializingBean
 * @see org.springframework.beans.factory.DisposableBean
 * @see org.springframework.beans.factory.FactoryBean
 * @see com.datastax.driver.core.Cluster
 */
@SuppressWarnings("unused")
public class CassandraClusterFactoryBean
		implements FactoryBean<Cluster>, InitializingBean, DisposableBean, BeanNameAware, PersistenceExceptionTranslator {

	public static final boolean DEFAULT_JMX_REPORTING_ENABLED = true;
	public static final boolean DEFAULT_METRICS_ENABLED = true;
	public static final boolean DEFAULT_SSL_ENABLED = false;

	public static final int DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS = 10;
	public static final int DEFAULT_PORT = 9042;

	public static final String DEFAULT_CONTACT_POINTS = "localhost";

	protected static final Logger log = LoggerFactory.getLogger(CassandraCqlClusterFactoryBean.class);

	private boolean jmxReportingEnabled = DEFAULT_JMX_REPORTING_ENABLED;
	private boolean metricsEnabled = DEFAULT_METRICS_ENABLED;
	private boolean sslEnabled = DEFAULT_SSL_ENABLED;

	private int maxSchemaAgreementWaitSeconds = DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS;
	private int port = DEFAULT_PORT;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	private @Nullable Cluster cluster;
	private @Nullable ClusterBuilderConfigurer clusterBuilderConfigurer;

	private @Nullable AddressTranslator addressTranslator;
	private @Nullable AuthProvider authProvider;
	private @Nullable CompressionType compressionType;
	private @Nullable Host.StateListener hostStateListener;
	private @Nullable LatencyTracker latencyTracker;

	private List<CreateKeyspaceSpecification> keyspaceCreations = new ArrayList<>();
	private List<AlterKeyspaceSpecification> keyspaceAlterations = new ArrayList<>();
	private List<DropKeyspaceSpecification> keyspaceDrops = new ArrayList<>();
	private Set<KeyspaceActionSpecification> keyspaceSpecifications = new HashSet<>();
	private List<KeyspaceActions> keyspaceActions = new ArrayList<>();

	private List<String> startupScripts = new ArrayList<>();
	private List<String> shutdownScripts = new ArrayList<>();

	private @Nullable LoadBalancingPolicy loadBalancingPolicy;
	private NettyOptions nettyOptions = NettyOptions.DEFAULT_INSTANCE;
	private @Nullable PoolingOptions poolingOptions;
	private @Nullable ProtocolVersion protocolVersion;
	private @Nullable QueryOptions queryOptions;
	private @Nullable ReconnectionPolicy reconnectionPolicy;
	private @Nullable RetryPolicy retryPolicy;
	private @Nullable SpeculativeExecutionPolicy speculativeExecutionPolicy;
	private @Nullable SocketOptions socketOptions;
	private @Nullable SSLOptions sslOptions;
	private @Nullable TimestampGenerator timestampGenerator;

	private @Nullable String beanName;
	private @Nullable String clusterName;
	private String contactPoints = DEFAULT_CONTACT_POINTS;
	private @Nullable String password;
	private @Nullable String username;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {

		Assert.hasText(contactPoints, "At least one server is required");

		Cluster.Builder clusterBuilder = newClusterBuilder();

		clusterBuilder.addContactPoints(StringUtils.commaDelimitedListToStringArray(contactPoints)).withPort(port);
		clusterBuilder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);

		Optional.ofNullable(compressionType).map(CassandraClusterFactoryBean::convertCompressionType)
				.ifPresent(clusterBuilder::withCompression);

		Optional.ofNullable(addressTranslator).ifPresent(clusterBuilder::withAddressTranslator);
		Optional.ofNullable(loadBalancingPolicy).ifPresent(clusterBuilder::withLoadBalancingPolicy);
		clusterBuilder.withNettyOptions(nettyOptions);
		Optional.ofNullable(poolingOptions).ifPresent(clusterBuilder::withPoolingOptions);
		Optional.ofNullable(protocolVersion).ifPresent(clusterBuilder::withProtocolVersion);
		Optional.ofNullable(queryOptions).ifPresent(clusterBuilder::withQueryOptions);
		Optional.ofNullable(reconnectionPolicy).ifPresent(clusterBuilder::withReconnectionPolicy);
		Optional.ofNullable(retryPolicy).ifPresent(clusterBuilder::withRetryPolicy);
		Optional.ofNullable(socketOptions).ifPresent(clusterBuilder::withSocketOptions);
		Optional.ofNullable(speculativeExecutionPolicy).ifPresent(clusterBuilder::withSpeculativeExecutionPolicy);
		Optional.ofNullable(timestampGenerator).ifPresent(clusterBuilder::withTimestampGenerator);

		if (authProvider != null) {
			clusterBuilder.withAuthProvider(authProvider);
		} else if (username != null) {
			clusterBuilder.withCredentials(username, password);
		}

		if (!jmxReportingEnabled) {
			clusterBuilder.withoutJMXReporting();
		}

		if (!metricsEnabled) {
			clusterBuilder.withoutMetrics();
		}

		if (sslEnabled) {
			if (sslOptions != null) {
				clusterBuilder.withSSL(sslOptions);
			} else {
				clusterBuilder.withSSL();
			}
		}

		Optional.ofNullable(resolveClusterName()).filter(StringUtils::hasText).ifPresent(clusterBuilder::withClusterName);

		if (clusterBuilderConfigurer != null) {
			clusterBuilderConfigurer.configure(clusterBuilder);
		}

		cluster = clusterBuilder.build();

		Optional.ofNullable(hostStateListener).ifPresent(cluster::register);
		Optional.ofNullable(latencyTracker).ifPresent(cluster::register);

		generateSpecificationsFromFactoryBeans();

		List<KeyspaceActionSpecification> startup = new ArrayList<>(keyspaceCreations.size() + keyspaceAlterations.size());
		startup.addAll(keyspaceCreations);
		startup.addAll(keyspaceAlterations);

		executeSpecsAndScripts(startup, startupScripts, cluster);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.Cluster#builder()
	 */
	Cluster.Builder newClusterBuilder() {
		return Cluster.builder();
	}

	/* (non-Javadoc) */
	@Nullable
	private String resolveClusterName() {
		return StringUtils.hasText(clusterName) ? clusterName : beanName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() {

		if (cluster != null) {

			executeSpecsAndScripts(keyspaceDrops, shutdownScripts, cluster);
			cluster.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public Cluster getObject() {
		return cluster;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<? extends Cluster> getObjectType() {
		return (cluster != null ? cluster.getClass() : Cluster.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	/**
	 * Examines the contents of all the KeyspaceSpecificationFactoryBeans and generates the proper KeyspaceSpecification
	 * from them.
	 */
	private void generateSpecificationsFromFactoryBeans() {

		generateSpecifications(keyspaceSpecifications);
		keyspaceActions.forEach(actions -> generateSpecifications(actions.getActions()));
	}

	private void generateSpecifications(Collection<KeyspaceActionSpecification> specifications) {

		specifications.forEach(keyspaceActionSpecification -> {

			if (keyspaceActionSpecification instanceof CreateKeyspaceSpecification) {
				keyspaceCreations.add((CreateKeyspaceSpecification) keyspaceActionSpecification);
			}

			if (keyspaceActionSpecification instanceof DropKeyspaceSpecification) {
				keyspaceDrops.add((DropKeyspaceSpecification) keyspaceActionSpecification);
			}

			if (keyspaceActionSpecification instanceof AlterKeyspaceSpecification) {
				keyspaceAlterations.add((AlterKeyspaceSpecification) keyspaceActionSpecification);
			}
		});
	}

	private void executeSpecsAndScripts(List<? extends KeyspaceActionSpecification> keyspaceActionSpecifications,
			List<String> scripts, Cluster cluster) {

		if (!CollectionUtils.isEmpty(keyspaceActionSpecifications) || !CollectionUtils.isEmpty(scripts)) {

			Session session = cluster.connect();

			try {
				CqlTemplate template = new CqlTemplate(session);

				keyspaceActionSpecifications
						.forEach(keyspaceActionSpecification -> template.execute(toCql(keyspaceActionSpecification)));

				scripts.forEach(template::execute);
			} finally {
				if (session != null) {
					session.close();
				}
			}
		}
	}

	private String toCql(KeyspaceActionSpecification specification) {

		if (specification instanceof CreateKeyspaceSpecification) {
			return new CreateKeyspaceCqlGenerator((CreateKeyspaceSpecification) specification).toCql();
		}

		if (specification instanceof DropKeyspaceSpecification) {
			return new DropKeyspaceCqlGenerator((DropKeyspaceSpecification) specification).toCql();
		}

		if (specification instanceof AlterKeyspaceSpecification) {
			return new AlterKeyspaceCqlGenerator((AlterKeyspaceSpecification) specification).toCql();
		}

		throw new IllegalArgumentException(
				"Unsupported specification type: " + ClassUtils.getQualifiedName(specification.getClass()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.BeanNameAware#setBeanName(String)
	 * @since 1.5
	 */
	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
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
	 * Set the {@link CompressionType}. Default is uncompressed.
	 *
	 * @param compressionType the {@link CompressionType} used by the new cluster.
	 */
	public void setCompressionType(@Nullable CompressionType compressionType) {
		this.compressionType = compressionType;
	}

	/**
	 * Set the {@link PoolingOptions} to configure the connection pooling behavior.
	 *
	 * @param poolingOptions the {@link PoolingOptions} used by the new cluster.
	 */
	public void setPoolingOptions(@Nullable PoolingOptions poolingOptions) {
		this.poolingOptions = poolingOptions;
	}

	/**
	 * Set the {@link ProtocolVersion}.
	 *
	 * @param protocolVersion the {@link ProtocolVersion} used by the new cluster.
	 * @since 1.4
	 */
	public void setProtocolVersion(@Nullable ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	/**
	 * Set the {@link SocketOptions} containing low-level socket options.
	 *
	 * @param socketOptions the {@link SocketOptions} used by the new cluster.
	 */
	public void setSocketOptions(@Nullable SocketOptions socketOptions) {
		this.socketOptions = socketOptions;
	}

	/**
	 * Set the {@link QueryOptions} to tune to defaults for individual queries.
	 *
	 * @param queryOptions the {@link QueryOptions} used by the new cluster.
	 */
	public void setQueryOptions(@Nullable QueryOptions queryOptions) {
		this.queryOptions = queryOptions;
	}

	/**
	 * Set the {@link AuthProvider}. Default is unauthenticated.
	 *
	 * @param authProvider the {@link AuthProvider} used by the new cluster.
	 */
	public void setAuthProvider(@Nullable AuthProvider authProvider) {
		this.authProvider = authProvider;
	}

	/**
	 * Set the {@link NettyOptions} used by a client to customize the driver's underlying Netty layer.
	 *
	 * @param nettyOptions the {@link NettyOptions} used by the new cluster.
	 * @since 1.5
	 */
	public void setNettyOptions(NettyOptions nettyOptions) {
		this.nettyOptions = nettyOptions;
	}

	/**
	 * Set the {@link LoadBalancingPolicy} that decides which Cassandra hosts to contact for each new query.
	 *
	 * @param loadBalancingPolicy the {@link LoadBalancingPolicy} used by the new cluster.
	 */
	public void setLoadBalancingPolicy(@Nullable LoadBalancingPolicy loadBalancingPolicy) {
		this.loadBalancingPolicy = loadBalancingPolicy;
	}

	/**
	 * Set the {@link ReconnectionPolicy} that decides how often the reconnection to a dead node is attempted.
	 *
	 * @param reconnectionPolicy the {@link ReconnectionPolicy} used by the new cluster.
	 */
	public void setReconnectionPolicy(@Nullable ReconnectionPolicy reconnectionPolicy) {
		this.reconnectionPolicy = reconnectionPolicy;
	}

	/**
	 * Set the {@link RetryPolicy} that defines a default behavior to adopt when a request fails.
	 *
	 * @param retryPolicy the {@link RetryPolicy} used by the new cluster.
	 */
	public void setRetryPolicy(@Nullable RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}

	/**
	 * Set whether metrics are enabled. Default is {@literal true}, see {@link #DEFAULT_METRICS_ENABLED}.
	 */
	public void setMetricsEnabled(boolean metricsEnabled) {
		this.metricsEnabled = metricsEnabled;
	}

	/**
	 * @return the {@link List} of {@link KeyspaceActions}.
	 */
	public List<KeyspaceActions> getKeyspaceActions() {
		return Collections.unmodifiableList(keyspaceActions);
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
	 * @return {@link List} of {@link CreateKeyspaceSpecification create keyspace specifications}.
	 */
	public List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return Collections.unmodifiableList(keyspaceCreations);
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
	 * @return the {@link List} of {@link DropKeyspaceSpecification drop keyspace specifications}.
	 */
	public List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return Collections.unmodifiableList(keyspaceDrops);
	}

	/**
	 * Set a {@link List} of raw {@link String CQL statements} that are executed when this factory is
	 * {@link #afterPropertiesSet() initialized}. Scripts are executed on a system session with no keyspace set, after
	 * executing {@link #setKeyspaceCreations(List)}.
	 *
	 * @param scripts the scripts to execute on startup
	 */
	public void setStartupScripts(List<String> scripts) {
		this.startupScripts = new ArrayList<>(scripts);
	}

	/**
	 * @return the startup scripts
	 */
	public List<String> getStartupScripts() {
		return Collections.unmodifiableList(startupScripts);
	}

	/**
	 * Set a {@link List} of raw {@link String CQL statements} that are executed when this factory is {@link #destroy()
	 * destroyed}. {@link DropKeyspaceSpecification Drop keyspace specifications} are executed on a system session with no
	 * keyspace set, after executing {@link #setKeyspaceDrops(List)}.
	 *
	 * @param scripts the scripts to execute on shutdown
	 */
	public void setShutdownScripts(List<String> scripts) {
		this.shutdownScripts = new ArrayList<>(scripts);
	}

	/**
	 * @return the shutdown scripts
	 */
	public List<String> getShutdownScripts() {
		return Collections.unmodifiableList(shutdownScripts);
	}

	/**
	 * @param keyspaceSpecifications The {@link KeyspaceActionSpecification} to set.
	 */
	public void setKeyspaceSpecifications(Set<KeyspaceActionSpecification> keyspaceSpecifications) {
		this.keyspaceSpecifications = new LinkedHashSet<>(keyspaceSpecifications);
	}

	/**
	 * @return the {@link KeyspaceActionSpecification} associated with this factory.
	 */
	public Set<KeyspaceActionSpecification> getKeyspaceSpecifications() {
		return Collections.unmodifiableSet(keyspaceSpecifications);
	}

	/**
	 * Set the username to use with {@link com.datastax.driver.core.PlainTextAuthProvider}.
	 *
	 * @param username The username to set.
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Set the username to use with {@link com.datastax.driver.core.PlainTextAuthProvider}.
	 *
	 * @param password The password to set.
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Set whether to use JMX reporting. Default is {@literal false}, see {@link #DEFAULT_JMX_REPORTING_ENABLED}.
	 *
	 * @param jmxReportingEnabled The jmxReportingEnabled to set.
	 */
	public void setJmxReportingEnabled(boolean jmxReportingEnabled) {
		this.jmxReportingEnabled = jmxReportingEnabled;
	}

	/**
	 * Set whether to use SSL. Default is plain, see {@link #DEFAULT_SSL_ENABLED}.
	 *
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

	/**
	 * Configures the address translator used by the new cluster to translate IP addresses received from Cassandra nodes
	 * into locally query-able addresses.
	 *
	 * @param addressTranslator {@link AddressTranslator} used by the new cluster.
	 * @see com.datastax.driver.core.Cluster.Builder#withAddressTranslator(AddressTranslator)
	 * @see com.datastax.driver.core.policies.AddressTranslator
	 * @since 1.5
	 */
	public void setAddressTranslator(@Nullable AddressTranslator addressTranslator) {
		this.addressTranslator = addressTranslator;
	}

	/**
	 * Sets the {@link ClusterBuilderConfigurer} used to apply additional configuration logic to the
	 * {@link com.datastax.driver.core.Cluster.Builder}. {@link ClusterBuilderConfigurer} is invoked after all provided
	 * options are configured. The factory will {@link Builder#build()} the {@link Cluster} after applying
	 * {@link ClusterBuilderConfigurer}.
	 *
	 * @param clusterBuilderConfigurer {@link ClusterBuilderConfigurer} used to configure the
	 *          {@link com.datastax.driver.core.Cluster.Builder}.
	 * @see org.springframework.data.cql.config.ClusterBuilderConfigurer
	 */
	public void setClusterBuilderConfigurer(@Nullable ClusterBuilderConfigurer clusterBuilderConfigurer) {
		this.clusterBuilderConfigurer = clusterBuilderConfigurer;
	}

	/**
	 * An optional name for the cluster instance. This name appears in JMX metrics. Defaults to the bean name.
	 *
	 * @param clusterName optional name for the cluster.
	 * @see com.datastax.driver.core.Cluster.Builder#withClusterName(String)
	 * @since 1.5
	 */
	public void setClusterName(@Nullable String clusterName) {
		this.clusterName = clusterName;
	}

	/**
	 * Sets the maximum time to wait for schema agreement before returning from a DDL query. The timeout is used to wait
	 * for all currently up hosts in the cluster to agree on the schema.
	 *
	 * @param seconds max schema agreement wait in seconds.
	 * @see com.datastax.driver.core.Cluster.Builder#withMaxSchemaAgreementWaitSeconds(int)
	 * @since 1.5
	 */
	public void setMaxSchemaAgreementWaitSeconds(int seconds) {
		this.maxSchemaAgreementWaitSeconds = seconds;
	}

	/**
	 * Configures the speculative execution policy to use for the new cluster.
	 *
	 * @param speculativeExecutionPolicy {@link SpeculativeExecutionPolicy} to use with the new cluster.
	 * @see com.datastax.driver.core.Cluster.Builder#withSpeculativeExecutionPolicy(SpeculativeExecutionPolicy)
	 * @see com.datastax.driver.core.policies.SpeculativeExecutionPolicy
	 * @since 1.5
	 */
	public void setSpeculativeExecutionPolicy(@Nullable SpeculativeExecutionPolicy speculativeExecutionPolicy) {
		this.speculativeExecutionPolicy = speculativeExecutionPolicy;
	}

	/**
	 * Configures the generator that will produce the client-side timestamp sent with each query.
	 *
	 * @param timestampGenerator {@link TimestampGenerator} used to produce a client-side timestamp sent with each query.
	 * @see com.datastax.driver.core.Cluster.Builder#withTimestampGenerator(TimestampGenerator)
	 * @see com.datastax.driver.core.TimestampGenerator
	 * @since 1.5
	 */
	public void setTimestampGenerator(@Nullable TimestampGenerator timestampGenerator) {
		this.timestampGenerator = timestampGenerator;
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

		throw new IllegalArgumentException(String.format("Unknown compression type [%s]", type));
	}
}
