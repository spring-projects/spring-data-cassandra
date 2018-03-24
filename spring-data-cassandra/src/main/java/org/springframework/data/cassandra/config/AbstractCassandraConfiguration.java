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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleTupleTypeFactory;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 */
@Configuration
public abstract class AbstractCassandraConfiguration extends AbstractClusterConfiguration
		implements BeanClassLoaderAware {

	private @Nullable ClassLoader beanClassLoader;

	/**
	 * Returns the initialized {@link Session} instance.
	 *
	 * @return the {@link Session}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected Session getRequiredSession() {

		CassandraSessionFactoryBean factoryBean = session();

		Session session = factoryBean.getObject();

		Assert.state(session != null, "Session factory not initialized");

		return session;
	}

	/**
	 * Creates a {@link CassandraSessionFactoryBean} that provides a Cassandra {@link com.datastax.driver.core.Session}.
	 * The lifecycle of {@link CassandraSessionFactoryBean} initializes the {@link #getSchemaAction() schema} in the
	 * {@link #getKeyspaceName() configured keyspace}.
	 *
	 * @return the {@link CassandraSessionFactoryBean}.
	 * @see #cluster()
	 * @see #cassandraConverter()
	 * @see #getKeyspaceName()
	 * @see #getSchemaAction()
	 * @see #getStartupScripts()
	 * @see #getShutdownScripts()
	 */
	@Bean
	public CassandraSessionFactoryBean session() {

		CassandraSessionFactoryBean session = new CassandraSessionFactoryBean();

		session.setCluster(getRequiredCluster());
		session.setConverter(cassandraConverter());
		session.setKeyspaceName(getKeyspaceName());
		session.setSchemaAction(getSchemaAction());
		session.setStartupScripts(getStartupScripts());
		session.setShutdownScripts(getShutdownScripts());

		return session;
	}

	/**
	 * Creates a {@link DefaultSessionFactory} using the configured {@link #session()} to be used with
	 * {@link org.springframework.data.cassandra.core.CassandraTemplate}.
	 *
	 * @return {@link SessionFactory} used to initialize the Template API.
	 * @since 2.0
	 */
	@Bean
	public SessionFactory sessionFactory() {
		return new DefaultSessionFactory(getRequiredSession());
	}

	/**
	 * Creates a {@link CassandraConverter} using the configured {@link #cassandraMapping()}. Will apply all specified
	 * {@link #customConversions()}.
	 *
	 * @return {@link CassandraConverter} used to convert Java and Cassandra value types during the mapping process.
	 * @see #cassandraMapping()
	 * @see #customConversions()
	 */
	@Bean
	public CassandraConverter cassandraConverter() {

		try {
			MappingCassandraConverter mappingCassandraConverter = new MappingCassandraConverter(cassandraMapping());

			mappingCassandraConverter.setCustomConversions(customConversions());

			return mappingCassandraConverter;
		} catch (ClassNotFoundException cause) {
			throw new IllegalStateException(cause);
		}
	}

	/**
	 * Return the {@link MappingContext} instance to map Entities to properties.
	 *
	 * @throws ClassNotFoundException if the Cassandra Entity class type identified by name cannot be found during the
	 *           scan.
	 * @see CassandraMappingContext
	 */
	@Bean
	public CassandraMappingContext cassandraMapping() throws ClassNotFoundException {

		Cluster cluster = getRequiredCluster();

		CassandraMappingContext mappingContext = new CassandraMappingContext(
				new SimpleUserTypeResolver(cluster, getKeyspaceName()), new SimpleTupleTypeFactory(cluster));

		Optional.ofNullable(this.beanClassLoader).ifPresent(mappingContext::setBeanClassLoader);

		mappingContext.setInitialEntitySet(getInitialEntitySet());

		CustomConversions customConversions = customConversions();

		mappingContext.setCustomConversions(customConversions);
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		return mappingContext;
	}

	/**
	 * Register custom {@link Converter}s in a {@link CustomConversions} object if required. These
	 * {@link CustomConversions} will be registered with the {@link #cassandraConverter()} and {@link #cassandraMapping()}
	 * . Returns an empty {@link CustomConversions} instance by default.
	 *
	 * @return must not be {@literal null}.
	 * @since 1.5
	 */
	@Bean
	public CustomConversions customConversions() {
		return new CassandraCustomConversions(Collections.emptyList());
	}

	/**
	 * Return the {@link Set} of initial entity classes. Scans by default the class path using
	 * {@link #getEntityBasePackages()}. Can be overriden by subclasses to skip class path scanning and return a fixed set
	 * of entity classes.
	 *
	 * @return {@link Set} of initial entity classes.
	 * @throws ClassNotFoundException if the entity scan fails.
	 * @see #getEntityBasePackages()
	 * @see CassandraEntityClassScanner
	 * @since 2.0
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
		return CassandraEntityClassScanner.scan(getEntityBasePackages());
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 *
	 * @throws Exception if the {@link com.datastax.driver.core.Session} could not be obtained.
	 */
	@Bean
	public CassandraAdminOperations cassandraTemplate() throws Exception {
		return new CassandraAdminTemplate(sessionFactory(), cassandraConverter());
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	/**
	 * Base packages to scan for entities annotated with {@link Table} annotations. By default, returns the package name
	 * of {@literal this} ({@code this.getClass().getPackage().getName()}. This method must never return {@literal null}.
	 */
	public String[] getEntityBasePackages() {
		return new String[] { getClass().getPackage().getName() };
	}

	/**
	 * Return the name of the keyspace to connect to.
	 *
	 * @return must not be {@literal null}.
	 */
	protected abstract String getKeyspaceName();

	/**
	 * The {@link SchemaAction} to perform at startup. Defaults to {@link SchemaAction#NONE}.
	 */
	public SchemaAction getSchemaAction() {
		return SchemaAction.NONE;
	}
}
