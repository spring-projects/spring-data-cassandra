/*
 * Copyright 2013-2021 the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Spring {@link Configuration} class used to configure a Cassandra client application {@link CqlSession} connected to a
 * Cassandra cluster. Enables a Cassandra Keyspace to be specified along with the ability to execute arbitrary CQL on
 * startup as well as shutdown.
 *
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 * @see org.springframework.beans.factory.BeanFactoryAware
 * @see org.springframework.context.annotation.Configuration
 */
@Configuration
public abstract class AbstractSessionConfiguration implements BeanFactoryAware {

	private @Nullable BeanFactory beanFactory;

	/**
	 * Configures a reference to the {@link BeanFactory}.
	 *
	 * @param beanFactory reference to the {@link BeanFactory}.
	 * @throws BeansException if the {@link BeanFactory} could not be initialized.
	 * @see org.springframework.beans.factory.BeanFactory
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	/**
	 * Returns the configured reference to the {@link BeanFactory}.
	 *
	 * @return the configured reference to the {@link BeanFactory}.
	 * @throws IllegalStateException if the {@link BeanFactory} reference was not configured.
	 * @see org.springframework.beans.factory.BeanFactory
	 */
	protected BeanFactory getBeanFactory() {

		Assert.state(this.beanFactory != null, "BeanFactory not initialized");

		return this.beanFactory;
	}

	/**
	 * Gets a required bean of the provided {@link Class type} from the {@link BeanFactory}.
	 *
	 * @param <T> {@link Class parameterized class type} of the bean.
	 * @param beanType {@link Class type} of the bean.
	 * @return a required bean of the given {@link Class type} from the {@link BeanFactory}.
	 * @see org.springframework.beans.factory.BeanFactory#getBean(Class)
	 * @see #getBeanFactory()
	 */
	protected <T> T requireBeanOfType(@NonNull Class<T> beanType) {
		return getBeanFactory().getBean(beanType);
	}

	/**
	 * Returns the {@link String name} of the cluster.
	 *
	 * @return the {@link String cluster name}; may be {@literal null}.
	 * @deprecated since 3.0, use {@link #getSessionName()} instead.
	 * @since 1.5
	 */
	@Nullable
	@Deprecated
	protected String getClusterName() {
		return null;
	}

	/**
	 * Return the name of the keyspace to connect to.
	 *
	 * @return must not be {@literal null}.
	 */
	protected abstract String getKeyspaceName();

	/**
	 * Returns the local data center name used for
	 * {@link com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy}, defaulting to {@code datacenter1}.
	 * Typically required when connecting a Cassandra cluster. Not required when using an Astra connection bundle.
	 *
	 * @return the local data center name. Can be {@literal null} when using an Astra connection bundle.
	 */
	@Nullable
	protected String getLocalDataCenter() {
		return "datacenter1";
	}

	/**
	 * Returns the session name.
	 *
	 * @return the session name; may be {@literal null}.
	 * @since 3.0
	 */
	@Nullable
	protected String getSessionName() {
		return null;
	}

	/**
	 * Returns the {@link CompressionType}.
	 *
	 * @return the {@link CompressionType}, may be {@literal null}.
	 */
	@Nullable
	protected CompressionType getCompressionType() {
		return null;
	}

	/**
	 * Returns the Cassandra contact points. Defaults to {@code localhost}
	 *
	 * @return the Cassandra contact points
	 * @see CqlSessionFactoryBean#DEFAULT_CONTACT_POINTS
	 */
	protected String getContactPoints() {
		return CqlSessionFactoryBean.DEFAULT_CONTACT_POINTS;
	}

	/**
	 * Returns the Cassandra port. Defaults to {@code 9042}.
	 *
	 * @return the Cassandra port
	 * @see CqlSessionFactoryBean#DEFAULT_PORT
	 */
	protected int getPort() {
		return CqlSessionFactoryBean.DEFAULT_PORT;
	}

	/**
	 * Returns the list of keyspace creations to be run right after initialization.
	 *
	 * @return the list of keyspace creations, may be empty but never {@code null}.
	 */
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return Collections.emptyList();
	}

	/**
	 * Returns the list of keyspace drops to be run before shutdown.
	 *
	 * @return the list of keyspace drops, may be empty but never {@code null}.
	 */
	protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return Collections.emptyList();
	}

	/**
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected CqlSession getRequiredSession() {
		return requireBeanOfType(CqlSession.class);
	}

	/**
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected SessionFactory getRequiredSessionFactory() {

		ObjectProvider<SessionFactory> beanProvider = getBeanFactory().getBeanProvider(SessionFactory.class);

		return beanProvider.getIfAvailable(() -> new DefaultSessionFactory(requireBeanOfType(CqlSession.class)));
	}

	/**
	 * Returns the {@link SessionBuilderConfigurer}.
	 *
	 * @return the {@link SessionBuilderConfigurer}; may be {@literal null}.
	 * @since 1.5
	 */
	@Nullable
	protected SessionBuilderConfigurer getSessionBuilderConfigurer() {
		return null;
	}

	/**
	 * Returns the {@link DriverConfigLoaderBuilderConfigurer}. The configuration gets applied after applying
	 * {@link System#getProperties() System Properties} config overrides and before
	 * {@link #getDriverConfigurationResource() the driver config file}.
	 *
	 * @return the {@link DriverConfigLoaderBuilderConfigurer}; may be {@literal null}.
	 * @since 3.1.2
	 */
	@Nullable
	protected DriverConfigLoaderBuilderConfigurer getDriverConfigLoaderBuilderConfigurer() {
		return null;
	}

	/**
	 * Returns the {@link Resource} pointing to a driver configuration file. The configuration file is applied after
	 * applying {@link System#getProperties() System Properties} and the configuration built by this configuration class.
	 *
	 * @return the {@link Resource}; may be {@literal null} if none provided.
	 * @since 3.1.2
	 * @see <a href="https://docs.datastax.com/en/developer/java-driver/4.9/manual/core/configuration/">Driver
	 *      Configuration</a>
	 */
	@Nullable
	protected Resource getDriverConfigurationResource() {
		return null;
	}

	/**
	 * Returns the list of CQL scripts to be run on startup after {@link #getKeyspaceCreations() Keyspace creations}
	 * and after initialization of the {@literal System} Keyspace.
	 *
	 * @return the list of CQL scripts to be run on startup; may be {@link Collections#emptyList() empty}
	 * but never {@literal null}.
	 * @deprecated since 3.0; Declare a
	 * {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer} bean instead.
	 */
	@Deprecated
	protected List<String> getStartupScripts() {
		return Collections.emptyList();
	}

	/**
	 * Returns the list of CQL scripts to be run on shutdown after {@link #getKeyspaceDrops() Keyspace drops}
	 * and right before shutdown of the {@code System} Keyspace.
	 *
	 * @return the list of CQL scripts to be run on shutdown; may be {@link Collections#emptyList() empty}
	 * but never {@literal null}.
	 * @deprecated since 3.0; Declare a
	 * {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer} bean instead.
	 */
	@Deprecated
	protected List<String> getShutdownScripts() {
		return Collections.emptyList();
	}

	/**
	 * Creates a {@link CqlSessionFactoryBean} that provides a Cassandra {@link CqlSession}.
	 *
	 * @return the {@link CqlSessionFactoryBean}.
	 * @see #getKeyspaceName()
	 * @see #getStartupScripts()
	 * @see #getShutdownScripts()
	 */
	@Bean
	public CqlSessionFactoryBean cassandraSession() {

		CqlSessionFactoryBean bean = new CqlSessionFactoryBean();

		bean.setContactPoints(getContactPoints());
		bean.setKeyspaceCreations(getKeyspaceCreations());
		bean.setKeyspaceDrops(getKeyspaceDrops());
		bean.setKeyspaceName(getKeyspaceName());
		bean.setKeyspaceStartupScripts(getStartupScripts());
		bean.setKeyspaceShutdownScripts(getShutdownScripts());
		bean.setLocalDatacenter(getLocalDataCenter());
		bean.setPort(getPort());
		bean.setSessionBuilderConfigurer(getSessionBuilderConfigurerWrapper());

		return bean;
	}

	private SessionBuilderConfigurer getSessionBuilderConfigurerWrapper() {

		SessionBuilderConfigurer sessionConfigurer = getSessionBuilderConfigurer();
		DriverConfigLoaderBuilderConfigurer driverConfigLoaderConfigurer = getDriverConfigLoaderBuilderConfigurer();
		Resource driverConfigFile = getDriverConfigurationResource();

		return sessionBuilder -> {

			ProgrammaticDriverConfigLoaderBuilder builder = new DefaultProgrammaticDriverConfigLoaderBuilder(() -> {

				CassandraDriverOptions options = new CassandraDriverOptions();

				if (StringUtils.hasText(getSessionName())) {
					options.add(DefaultDriverOption.SESSION_NAME, getSessionName());
				} else if (StringUtils.hasText(getClusterName())) {
					options.add(DefaultDriverOption.SESSION_NAME, getClusterName());
				}

				CompressionType compressionType = getCompressionType();

				if (compressionType != null) {
					options.add(DefaultDriverOption.PROTOCOL_COMPRESSION, compressionType);
				}

				ConfigFactory.invalidateCaches();

				Config config = ConfigFactory.defaultOverrides() //
						.withFallback(options.build());

				if (driverConfigFile != null) {
					try {
						config = config
								.withFallback(ConfigFactory.parseReader(new InputStreamReader(driverConfigFile.getInputStream())));
					} catch (IOException e) {
						throw new IllegalStateException(String.format("Cannot parse driver config file %s", driverConfigFile), e);
					}
				}

				return config.withFallback(ConfigFactory.defaultReference());

			}, DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);

			if (driverConfigLoaderConfigurer != null) {
				driverConfigLoaderConfigurer.configure(builder);
			}

			sessionBuilder.withConfigLoader(builder.build());

			if (sessionConfigurer != null) {
				return sessionConfigurer.configure(sessionBuilder);
			}

			return sessionBuilder;
		};
	}

	/**
	 * Creates a {@link CqlTemplate} configured with {@link #getRequiredSessionFactory()}.
	 *
	 * @return the {@link CqlTemplate}.
	 * @see #getRequiredSession()
	 */
	@Bean
	public CqlTemplate cqlTemplate() {
		return new CqlTemplate(getRequiredSessionFactory());
	}

	private static class CassandraDriverOptions {

		private final Map<String, String> options = new LinkedHashMap<>();

		private CassandraDriverOptions add(DriverOption option, String value) {
			this.options.put(option.getPath(), value);
			return this;
		}

		private CassandraDriverOptions add(DriverOption option, Enum<?> value) {
			return add(option, value.name());
		}

		private Config build() {
			return ConfigFactory.parseMap(this.options, "Environment");
		}

	}
}
