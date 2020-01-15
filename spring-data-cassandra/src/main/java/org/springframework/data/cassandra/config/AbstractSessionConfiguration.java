/*
 * Copyright 2013-2020 the original author or authors.
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
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
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
 * @see org.springframework.context.annotation.Configuration
 */
@Configuration
public abstract class AbstractSessionConfiguration implements BeanFactoryAware {

	private @Nullable BeanFactory beanFactory;

	/**
	 * Return the name of the keyspace to connect to.
	 *
	 * @return must not be {@literal null}.
	 */
	protected abstract String getKeyspaceName();

	/**
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected SessionFactory getRequiredSessionFactory() {

		ObjectProvider<SessionFactory> beanProvider = beanFactory.getBeanProvider(SessionFactory.class);

		return beanProvider.getIfAvailable(() -> new DefaultSessionFactory(beanFactory.getBean(CqlSession.class)));
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
	 * Returns the cluster name.
	 *
	 * @return the cluster name; may be {@literal null}.
	 * @since 1.5
	 * @deprecated since 3.0, use {@link #getSessionName()} instead.
	 */
	@Nullable
	@Deprecated
	protected String getClusterName() {
		return null;
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
	 * Returns the local data center name used for
	 * {@link com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy}.
	 *
	 * @return the local data center name.
	 */
	protected String getLocalDataCenter() {
		return null;
	}

	/**
	 * Returns the list of startup scripts to be run after {@link #getKeyspaceCreations() keyspace creations} and after
	 * initialization in the {@code system} keyspace.
	 *
	 * @return the list of startup scripts, may be empty but never {@code null}.
	 * @deprecated since 3.0, declare a
	 *             {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer} bean.
	 */
	@Deprecated
	protected List<String> getStartupScripts() {
		return Collections.emptyList();
	}

	/**
	 * Returns the list of shutdown scripts to be run after {@link #getKeyspaceDrops() keyspace drops} and right before
	 * shutdown in the {@code system} keyspace.
	 *
	 * @return the list of shutdown scripts, may be empty but never {@code null}.
	 * @deprecated since 3.0, declare a
	 *             {@link org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer} bean.
	 */
	@Deprecated
	protected List<String> getShutdownScripts() {
		return Collections.emptyList();
	}

	/**
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected CqlSession getRequiredSession() {

		Assert.state(beanFactory != null, "BeanFactory not initialized");

		return beanFactory.getBean(CqlSession.class);
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
		bean.setPort(getPort());
		bean.setLocalDatacenter(getLocalDataCenter());

		bean.setKeyspaceCreations(getKeyspaceCreations());
		bean.setKeyspaceDrops(getKeyspaceDrops());

		bean.setSessionSessionBuilderConfigurer(getBuilderConfigurer());

		bean.setKeyspaceName(getKeyspaceName());
		bean.setKeyspaceStartupScripts(getStartupScripts());
		bean.setKeyspaceShutdownScripts(getShutdownScripts());

		return bean;
	}

	private SessionBuilderConfigurer getBuilderConfigurer() {

		SessionBuilderConfigurer configurer = getSessionBuilderConfigurer();

		return sessionBuilder -> {

			ProgrammaticDriverConfigLoaderBuilder builder = new DefaultProgrammaticDriverConfigLoaderBuilder(() -> {

				CassandraDriverOptions options = new CassandraDriverOptions();

				if (StringUtils.hasText(getClusterName())) {
					options.add(DefaultDriverOption.SESSION_NAME, getClusterName());
				} else if (StringUtils.hasText(getSessionName())) {
					options.add(DefaultDriverOption.SESSION_NAME, getSessionName());
				}

				CompressionType compressionType = getCompressionType();
				if (compressionType != null) {
					options.add(DefaultDriverOption.PROTOCOL_COMPRESSION, compressionType);
				}

				ConfigFactory.invalidateCaches();
				return ConfigFactory.defaultOverrides().withFallback(options.build())
						.withFallback(ConfigFactory.defaultReference()).resolve();

			}, DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);

			sessionBuilder.withConfigLoader(builder.build());

			if (configurer != null) {
				return configurer.configure(sessionBuilder);
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

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	private static class CassandraDriverOptions {

		private final Map<String, String> options = new LinkedHashMap<>();

		private CassandraDriverOptions add(DriverOption option, String value) {
			String key = createKeyFor(option);
			this.options.put(key, value);
			return this;
		}

		private CassandraDriverOptions add(DriverOption option, int value) {
			return add(option, String.valueOf(value));
		}

		private CassandraDriverOptions add(DriverOption option, Enum<?> value) {
			return add(option, value.name());
		}

		private CassandraDriverOptions add(DriverOption option, List<String> values) {
			for (int i = 0; i < values.size(); i++) {
				this.options.put(String.format("%s.%s", createKeyFor(option), i), values.get(i));
			}
			return this;
		}

		private Config build() {
			return ConfigFactory.parseMap(this.options, "Environment");
		}

		private static String createKeyFor(DriverOption option) {
			return String.format("%s.%s", DefaultDriverConfigLoader.DEFAULT_ROOT_PATH, option.getPath());
		}
	}

}
