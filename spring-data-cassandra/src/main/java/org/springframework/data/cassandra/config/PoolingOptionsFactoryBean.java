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

import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;

/**
 * Spring {@link FactoryBean} for the Cassandra Java driver {@link PoolingOptions}.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 * @author John Blum
 * @see org.springframework.beans.factory.FactoryBean
 * @see org.springframework.beans.factory.InitializingBean
 * @see com.datastax.driver.core.PoolingOptions
 */
@SuppressWarnings({ "unused", "WeakerAccess" })
public class PoolingOptionsFactoryBean implements FactoryBean<PoolingOptions>, InitializingBean {

	private static final PoolingOptions DEFAULT = new PoolingOptions();

	// Compatibility between 3.1.1 and earlier Cassandra driver versions
	private static final Optional<Method> SET_MAX_QUEUE_SIZE;

	// Compatibility between 3.1.1 and earlier Cassandra driver versions
	private static final Optional<Method> GET_MAX_QUEUE_SIZE;

	static {
		SET_MAX_QUEUE_SIZE = Optional
				.ofNullable(ReflectionUtils.findMethod(PoolingOptions.class, "setMaxQueueSize", int.class));
		GET_MAX_QUEUE_SIZE = Optional.ofNullable(ReflectionUtils.findMethod(PoolingOptions.class, "getMaxQueueSize"));
	}

	private @Nullable Executor initializationExecutor;

	private int heartbeatIntervalSeconds;
	private int idleTimeoutSeconds;
	private @Nullable Integer localCoreConnections;
	private @Nullable Integer localMaxConnections;
	private @Nullable Integer localMaxSimultaneousRequests;
	private @Nullable Integer localMinSimultaneousRequests;

	// Deprecated since Cassandra Driver 3.1.1
	private int poolTimeoutMilliseconds;

	// Available since Cassandra Driver 3.1.1
	private int maxQueueSize;
	private @Nullable Integer remoteCoreConnections;
	private @Nullable Integer remoteMaxConnections;
	private @Nullable Integer remoteMaxSimultaneousRequests;
	private @Nullable Integer remoteMinSimultaneousRequests;

	private @Nullable PoolingOptions poolingOptions;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {

		poolingOptions = configureRemoteHostDistancePoolingOptions(
				configureLocalHostDistancePoolingOptions(newPoolingOptions()));

		if (heartbeatIntervalSeconds != DEFAULT.getHeartbeatIntervalSeconds()) {
			poolingOptions.setHeartbeatIntervalSeconds(heartbeatIntervalSeconds);
		}

		if (idleTimeoutSeconds != DEFAULT.getIdleTimeoutSeconds()) {
			poolingOptions.setIdleTimeoutSeconds(idleTimeoutSeconds);
		}

		if (initializationExecutor != null) {
			poolingOptions.setInitializationExecutor(initializationExecutor);
		}

		if (poolTimeoutMilliseconds != DEFAULT.getPoolTimeoutMillis()) {
			poolingOptions.setPoolTimeoutMillis(poolTimeoutMilliseconds);
		}

		if (!isDefaultMaxQueueSize()) {
			SET_MAX_QUEUE_SIZE.ifPresent(method -> invokeMethod(method, poolingOptions, maxQueueSize));
		}
	}

	private boolean isDefaultMaxQueueSize() {

		return GET_MAX_QUEUE_SIZE.map(method -> {

			Integer defaultMaxQueueSize = (Integer) invokeMethod(method, poolingOptions);
			return defaultMaxQueueSize != null && defaultMaxQueueSize == maxQueueSize;
		}).orElse(false);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.PoolingOptions
	 */
	PoolingOptions newPoolingOptions() {
		return new PoolingOptions();
	}

	/**
	 * Constructs and returns a {@link PoolingOptionsFactoryBean.HostDistancePoolingOptions} instance initialized with the
	 * {@link HostDistance#LOCAL}-based {@link PoolingOptions} as configured on this {@link PoolingOptionsFactoryBean}.
	 *
	 * @return {@link PoolingOptionsFactoryBean.HostDistancePoolingOptions} initialized with this
	 *         {@link PoolingOptionsFactoryBean}'s {@link HostDistance#LOCAL}-based {@link PoolingOptions}.
	 * @see com.datastax.driver.core.HostDistance#LOCAL
	 * @see com.datastax.driver.core.PoolingOptions
	 * @see PoolingOptionsFactoryBean.HostDistancePoolingOptions
	 * @see PoolingOptionsFactoryBean.LocalHostDistancePoolingOptions
	 */
	protected HostDistancePoolingOptions newLocalHostDistancePoolingOptions() {
		return new LocalHostDistancePoolingOptions(getLocalCoreConnections(), getLocalMaxConnections(),
				getLocalMaxSimultaneousRequests(), getLocalMinSimultaneousRequests());
	}

	/**
	 * Constructs and returns a {@link PoolingOptionsFactoryBean.HostDistancePoolingOptions} instance initialized with the
	 * {@link HostDistance#REMOTE}-based {@link PoolingOptions} as configured on this {@link PoolingOptionsFactoryBean}.
	 *
	 * @return {@link PoolingOptionsFactoryBean.HostDistancePoolingOptions} initialized with this
	 *         {@link PoolingOptionsFactoryBean}'s {@link HostDistance#REMOTE}-based {@link PoolingOptions}.
	 * @see com.datastax.driver.core.HostDistance#REMOTE
	 * @see com.datastax.driver.core.PoolingOptions
	 * @see PoolingOptionsFactoryBean.HostDistancePoolingOptions
	 * @see PoolingOptionsFactoryBean.RemoteHostDistancePoolingOptions
	 */
	protected HostDistancePoolingOptions newRemoteHostDistancePoolingOptions() {
		return new RemoteHostDistancePoolingOptions(getRemoteCoreConnections(), getRemoteMaxConnections(),
				getRemoteMaxSimultaneousRequests(), getRemoteMinSimultaneousRequests());
	}

	/**
	 * Configures the {@link HostDistance#LOCAL} connection settings on the given {@link PoolingOptions}.
	 *
	 * @param poolingOptions the {@link PoolingOptions} to configure.
	 * @return the given {@link PoolingOptions}.
	 * @see com.datastax.driver.core.HostDistance#LOCAL
	 * @see com.datastax.driver.core.PoolingOptions
	 * @see #newLocalHostDistancePoolingOptions()
	 */
	protected PoolingOptions configureLocalHostDistancePoolingOptions(PoolingOptions poolingOptions) {
		return newLocalHostDistancePoolingOptions().configure(poolingOptions);
	}

	/**
	 * Configures the {@link HostDistance#REMOTE} connection settings on the given {@link PoolingOptions}.
	 *
	 * @param poolingOptions the {@link PoolingOptions} to configure.
	 * @return the given {@link PoolingOptions}.
	 * @see com.datastax.driver.core.HostDistance#REMOTE
	 * @see com.datastax.driver.core.PoolingOptions
	 * @see #newRemoteHostDistancePoolingOptions()
	 */
	protected PoolingOptions configureRemoteHostDistancePoolingOptions(PoolingOptions poolingOptions) {
		return newRemoteHostDistancePoolingOptions().configure(poolingOptions);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public PoolingOptions getObject() throws Exception {
		return poolingOptions;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<?> getObjectType() {
		return (poolingOptions != null ? poolingOptions.getClass() : PoolingOptions.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * Sets the heart beat interval, after which a message is sent on an idle connection to make sure it's still alive.
	 *
	 * @param heartbeatIntervalSeconds interval in seconds between heartbeat messages to keep idle connections alive.
	 */
	public void setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
		this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
	}

	/**
	 * Gets the heart beat interval, after which a message is sent on an idle connection to make sure it's still alive.
	 *
	 * @return the {@code heartbeatIntervalSeconds}.
	 */
	public int getHeartbeatIntervalSeconds() {
		return heartbeatIntervalSeconds;
	}

	/**
	 * Sets the timeout before an idle connection is removed.
	 *
	 * @param idleTimeoutSeconds idle timeout in seconds before a connection is removed.
	 */
	public void setIdleTimeoutSeconds(int idleTimeoutSeconds) {
		this.idleTimeoutSeconds = idleTimeoutSeconds;
	}

	/**
	 * Get the timeout before an idle connection is removed.
	 *
	 * @return the {@code idleTimeoutSeconds}.
	 */
	public int getIdleTimeoutSeconds() {
		return idleTimeoutSeconds;
	}

	/**
	 * Sets the {@link Executor} to use for connection initialization.
	 *
	 * @param initializationExecutor {@link Executor} used to initialize the connection.
	 */
	public void setInitializationExecutor(Executor initializationExecutor) {
		this.initializationExecutor = initializationExecutor;
	}

	/**
	 * Gets the {@link Executor} to use for connection initialization.
	 *
	 * @return the {@code initializationExecutor}.
	 */
	@Nullable
	public Executor getInitializationExecutor() {
		return initializationExecutor;
	}

	/**
	 * Sets the timeout when trying to acquire a connection from a host's pool.
	 *
	 * @param poolTimeoutMilliseconds timeout in milliseconds used to acquire a connection from the host's pool.
	 */
	public void setPoolTimeoutMilliseconds(int poolTimeoutMilliseconds) {
		this.poolTimeoutMilliseconds = poolTimeoutMilliseconds;
	}

	/**
	 * Gets the timeout when trying to acquire a connection from a host's pool.
	 *
	 * @return the {@code poolTimeoutMilliseconds}.
	 */
	public int getPoolTimeoutMilliseconds() {
		return poolTimeoutMilliseconds;
	}

	/**
	 * Sets the maximum number of requests that get enqueued if no connection is available.
	 *
	 * @param maxQueueSize maximum number of requests that get enqueued if no connection is available.
	 */
	public void setMaxQueueSize(Integer maxQueueSize) {
		this.maxQueueSize = maxQueueSize;
	}

	/**
	 * Gets the maximum number of requests that get enqueued if no connection is available.
	 *
	 * @return the {@code maxQueueSize}.
	 */
	public Integer getMaxQueueSize() {
		return maxQueueSize;
	}

	/**
	 * Sets the core number of connections per host for the {@link HostDistance#LOCAL} scope.
	 *
	 * @param localCoreConnections core number of local connections per host.
	 */
	public void setLocalCoreConnections(@Nullable Integer localCoreConnections) {
		this.localCoreConnections = localCoreConnections;
	}

	/**
	 * Gets the core number of connections per host for the {@link HostDistance#LOCAL} scope.
	 *
	 * @return the {@code localCoreConnections).
	 */
	@Nullable
	public Integer getLocalCoreConnections() {
		return localCoreConnections;
	}

	/**
	 * Sets the maximum number of connections per host for the {@link HostDistance#LOCAL} scope.
	 *
	 * @param localMaxConnections max number of local connections per host.
	 */
	public void setLocalMaxConnections(@Nullable Integer localMaxConnections) {
		this.localMaxConnections = localMaxConnections;
	}

	/**
	 * Gets the maximum number of connections per host for the {@link HostDistance#LOCAL} scope.
	 *
	 * @return the {@code localMaxConnections}.
	 */
	@Nullable
	public Integer getLocalMaxConnections() {
		return localMaxConnections;
	}

	/**
	 * Sets the maximum number of requests per connection for the {@link HostDistance#LOCAL} scope.
	 *
	 * @param localMaxSimultaneousRequests max number of requests for local connections.
	 */
	public void setLocalMaxSimultaneousRequests(@Nullable Integer localMaxSimultaneousRequests) {
		this.localMaxSimultaneousRequests = localMaxSimultaneousRequests;
	}

	/**
	 * Gets the maximum number of requests per connection for the {@link HostDistance#LOCAL} scope.
	 *
	 * @return the {@code localMaxSimultaneousRequests}.
	 */
	@Nullable
	public Integer getLocalMaxSimultaneousRequests() {
		return localMaxSimultaneousRequests;
	}

	/**
	 * Sets the threshold that triggers the creation of a new connection to a host for the {@link HostDistance#LOCAL}
	 * scope.
	 *
	 * @param localMinSimultaneousRequests threshold triggering the creation of local connections to a host.
	 */
	public void setLocalMinSimultaneousRequests(@Nullable Integer localMinSimultaneousRequests) {
		this.localMinSimultaneousRequests = localMinSimultaneousRequests;
	}

	/**
	 * Gets the threshold that triggers the creation of a new connection to a host for the {@link HostDistance#LOCAL}
	 * scope.
	 *
	 * @return the {@code localMinSimultaneousRequests}.
	 */
	@Nullable
	public Integer getLocalMinSimultaneousRequests() {
		return localMinSimultaneousRequests;
	}

	/**
	 * Sets the core number of connections per host for the {@link HostDistance#REMOTE} scope.
	 *
	 * @param remoteCoreConnections core number of remote connections per host.
	 */
	public void setRemoteCoreConnections(@Nullable Integer remoteCoreConnections) {
		this.remoteCoreConnections = remoteCoreConnections;
	}

	/**
	 * Gets the core number of connections per host for the {@link HostDistance#REMOTE} scope.
	 *
	 * @return the {@code remoteCoreConnections).
	 */
	@Nullable
	public Integer getRemoteCoreConnections() {
		return remoteCoreConnections;
	}

	/**
	 * Sets the maximum number of connections per host for the {@link HostDistance#REMOTE} scope.
	 *
	 * @param remoteMaxConnections max number of remote connections per host.
	 */
	public void setRemoteMaxConnections(@Nullable Integer remoteMaxConnections) {
		this.remoteMaxConnections = remoteMaxConnections;
	}

	/**
	 * Gets the maximum number of connections per host for the {@link HostDistance#REMOTE} scope.
	 *
	 * @return the {@code remoteMaxConnections}.
	 */
	@Nullable
	public Integer getRemoteMaxConnections() {
		return remoteMaxConnections;
	}

	/**
	 * Sets the maximum number of requests per connection for the {@link HostDistance#REMOTE} scope.
	 *
	 * @param remoteMaxSimultaneousRequests max number of requests for local connections.
	 */
	public void setRemoteMaxSimultaneousRequests(@Nullable Integer remoteMaxSimultaneousRequests) {
		this.remoteMaxSimultaneousRequests = remoteMaxSimultaneousRequests;
	}

	/**
	 * Gets the maximum number of requests per connection for the {@link HostDistance#REMOTE} scope.
	 *
	 * @return the {@code remoteMaxSimultaneousRequests}.
	 */
	@Nullable
	public Integer getRemoteMaxSimultaneousRequests() {
		return remoteMaxSimultaneousRequests;
	}

	/**
	 * Sets the threshold that triggers the creation of a new connection to a host for the {@link HostDistance#REMOTE}
	 * scope.
	 *
	 * @param remoteMinSimultaneousRequests threshold triggering the creation of remote connections to a host.
	 */
	public void setRemoteMinSimultaneousRequests(@Nullable Integer remoteMinSimultaneousRequests) {
		this.remoteMinSimultaneousRequests = remoteMinSimultaneousRequests;
	}

	/**
	 * Gets the threshold that triggers the creation of a new connection to a host for the {@link HostDistance#REMOTE}
	 * scope.
	 *
	 * @return the {@code remoteMinSimultaneousRequests}.
	 */
	@Nullable
	public Integer getRemoteMinSimultaneousRequests() {
		return remoteMinSimultaneousRequests;
	}

	/**
	 * The HostDistancePoolingOptions class models the {@link PoolingOptions} state and connection settings for a
	 * particular {@link HostDistance}.
	 *
	 * @see com.datastax.driver.core.HostDistance
	 * @see com.datastax.driver.core.PoolingOptions
	 */
	protected static abstract class HostDistancePoolingOptions {

		private final @Nullable Integer coreConnectionsPerHost;
		private final @Nullable Integer maxConnectionsPerHost;
		private final @Nullable Integer maxRequestsPerConnection;
		private final @Nullable Integer newConnectionThreshold;

		/**
		 * Constructs an instance of {@link HostDistancePoolingOptions} with {@link PoolingOptions} connection settings
		 * specific to a particular {@link HostDistance}.
		 *
		 * @param coreConnectionsPerHost core number of connections per host.
		 * @param maxConnectionsPerHost maximum number of connections per host.
		 * @param maxRequestsPerConnection maximum number of requests per connection.
		 * @param newConnectionThreshold threshold that triggers the creation of a new connection to a host.
		 */
		protected HostDistancePoolingOptions(@Nullable Integer coreConnectionsPerHost,
				@Nullable Integer maxConnectionsPerHost, @Nullable Integer maxRequestsPerConnection,
				@Nullable Integer newConnectionThreshold) {

			this.coreConnectionsPerHost = coreConnectionsPerHost;
			this.maxConnectionsPerHost = maxConnectionsPerHost;
			this.maxRequestsPerConnection = maxRequestsPerConnection;
			this.newConnectionThreshold = newConnectionThreshold;
		}

		/**
		 * Returns the {@link HostDistance} used to configure the specific {@link PoolingOptions} connection settings.
		 *
		 * @return a {@link HostDistance} used to configure the specific {@link PoolingOptions} connection settings.
		 * @see com.datastax.driver.core.HostDistance
		 */
		protected abstract HostDistance getHostDistance();

		/*
		 * (non-Javadoc)
		 * @see com.datastax.driver.core.PoolingOptions#setCoreConnectionsPerHost(HostDistance, int)
		 */
		void setCoreConnectionsPerHost(PoolingOptions poolingOptions) {
			if (coreConnectionsPerHost != null) {
				poolingOptions.setCoreConnectionsPerHost(getHostDistance(), coreConnectionsPerHost);
			}
		}

		/**
		 * Returns the core number of connections per host.
		 *
		 * @return {@code coreConnectionsPerHost}.
		 * @see com.datastax.driver.core.PoolingOptions#getCoreConnectionsPerHost(HostDistance)
		 * @see #getHostDistance()
		 */
		@Nullable
		protected Integer getCoreConnectionsPerHost() {
			return coreConnectionsPerHost;
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.driver.core.PoolingOptions#setMaxConnectionsPerHost(HostDistance, int)
		 */
		void setMaxConnectionsPerHost(PoolingOptions poolingOptions) {
			if (maxConnectionsPerHost != null) {
				poolingOptions.setMaxConnectionsPerHost(getHostDistance(), maxConnectionsPerHost);
			}
		}

		/**
		 * Returns the maximum number of connections per host.
		 *
		 * @return {@code maxConnectionsPerHost}.
		 * @see com.datastax.driver.core.PoolingOptions#getMaxConnectionsPerHost(HostDistance)
		 * @see #getHostDistance()
		 */
		@Nullable
		protected Integer getMaxConnectionsPerHost() {
			return maxConnectionsPerHost;
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.driver.core.PoolingOptions#setMaxRequestsPerConnection(HostDistance, int)
		 */
		void setMaxRequestsPerConnection(PoolingOptions poolingOptions) {
			if (maxRequestsPerConnection != null) {
				poolingOptions.setMaxRequestsPerConnection(getHostDistance(), maxRequestsPerConnection);
			}
		}

		/**
		 * Returns the maximum number of requests per connection.
		 *
		 * @return {@code maxRequestsPerConnection}.
		 * @see com.datastax.driver.core.PoolingOptions#getMaxRequestsPerConnection(HostDistance)
		 * @see #getHostDistance()
		 */
		@Nullable
		protected Integer getMaxRequestsPerConnection() {
			return maxRequestsPerConnection;
		}

		/*
		 * If the new min is greater than the current max, set the current max to the new min first.
		 * This is enforced by the DSE Driver so you cannot set a new min/max together if either one falls outside
		 * of the default 25-100 range.
		 *
		 * @see com.datastax.driver.core.PoolingOptions#setNewConnectionThreshold(HostDistance, int)
		 */
		void setNewConnectionThreshold(PoolingOptions poolingOptions) {

			if (newConnectionThreshold != null) {
				int currentNewConnectionThreshold = poolingOptions.getNewConnectionThreshold(getHostDistance());

				if (currentNewConnectionThreshold < newConnectionThreshold) {
					poolingOptions.setNewConnectionThreshold(getHostDistance(), newConnectionThreshold);
				}
			}
		}

		/**
		 * Returns the threshold that triggers the creation of a new connection to a host.
		 *
		 * @return {@code newConnectionThreshold}.
		 * @see com.datastax.driver.core.PoolingOptions#getNewConnectionThreshold(HostDistance)
		 * @see #getHostDistance()
		 */
		@Nullable
		protected Integer getNewConnectionThreshold() {
			return newConnectionThreshold;
		}

		/*
		 * (non-Javadoc)
		 * @see com.datastax.driver.core.PoolingOptions
		 */
		PoolingOptions configure(PoolingOptions poolingOptions) {

			// order is important here; max properties must be set first
			setMaxConnectionsPerHost(poolingOptions);
			setCoreConnectionsPerHost(poolingOptions);
			setMaxRequestsPerConnection(poolingOptions);
			setNewConnectionThreshold(poolingOptions);

			return poolingOptions;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see HostDistancePoolingOptions
	 * @see com.datastax.driver.core.PoolingOptions
	 * @see com.datastax.driver.core.HostDistance#LOCAL
	 */
	static class LocalHostDistancePoolingOptions extends HostDistancePoolingOptions {

		/**
		 * Constructs an instance of {@link LocalHostDistancePoolingOptions} initialized with {@link PoolingOptions} based
		 * on {@link HostDistance#LOCAL}.
		 *
		 * @param coreConnectionsPerHost core number of connections per host.
		 * @param maxConnectionsPerHost maximum number of connections per host.
		 * @param maxRequestsPerConnection maximum number of requests per connection.
		 * @param newConnectionThreshold threshold that triggers the creation of a new connection to a host.
		 */
		LocalHostDistancePoolingOptions(@Nullable Integer coreConnectionsPerHost, @Nullable Integer maxConnectionsPerHost,
				@Nullable Integer maxRequestsPerConnection, @Nullable Integer newConnectionThreshold) {

			super(coreConnectionsPerHost, maxConnectionsPerHost, maxRequestsPerConnection, newConnectionThreshold);
		}

		/**
		 * Returns {@link HostDistance#LOCAL} to configure local-based {@link PoolingOptions} connection settings.
		 *
		 * @return {@link HostDistance#LOCAL} to configure local-based {@link PoolingOptions} connection settings.
		 * @see com.datastax.driver.core.HostDistance#LOCAL
		 */
		@Override
		protected HostDistance getHostDistance() {
			return HostDistance.LOCAL;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see HostDistancePoolingOptions
	 * @see com.datastax.driver.core.PoolingOptions
	 * @see com.datastax.driver.core.HostDistance#REMOTE
	 */
	static class RemoteHostDistancePoolingOptions extends HostDistancePoolingOptions {

		/**
		 * Constructs an instance of {@link RemoteHostDistancePoolingOptions} initialized with {@link PoolingOptions} based
		 * on {@link HostDistance#REMOTE}.
		 *
		 * @param coreConnectionsPerHost core number of connections per host.
		 * @param maxConnectionsPerHost maximum number of connections per host.
		 * @param maxRequestsPerConnection maximum number of requests per connection.
		 * @param newConnectionThreshold threshold that triggers the creation of a new connection to a host.
		 */
		RemoteHostDistancePoolingOptions(@Nullable Integer coreConnectionsPerHost, @Nullable Integer maxConnectionsPerHost,
				@Nullable Integer maxRequestsPerConnection, @Nullable Integer newConnectionThreshold) {

			super(coreConnectionsPerHost, maxConnectionsPerHost, maxRequestsPerConnection, newConnectionThreshold);
		}

		/**
		 * Returns {@link HostDistance#REMOTE} to configure remote-based {@link PoolingOptions} connection settings.
		 *
		 * @return {@link HostDistance#REMOTE} to configure remote-based {@link PoolingOptions} connection settings.
		 * @see com.datastax.driver.core.HostDistance#REMOTE
		 */
		@Override
		protected HostDistance getHostDistance() {
			return HostDistance.REMOTE;
		}
	}
}
