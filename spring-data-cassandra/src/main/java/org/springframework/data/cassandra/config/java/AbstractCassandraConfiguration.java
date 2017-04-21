/*
 * Copyright 2013-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config.java;

import java.util.Collections;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.cassandra.config.java.AbstractClusterConfiguration;
import org.springframework.cassandra.core.session.DefaultSessionFactory;
import org.springframework.cassandra.core.session.SessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;

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

	private ClassLoader beanClassLoader;

	/**
	 * Creates a {@link CassandraSessionFactoryBean} that provides a Cassandra {@link com.datastax.driver.core.Session}.
	 * The lifecycle of {@link CassandraSessionFactoryBean} initializes the {@link #getSchemaAction() schema} in the
	 * {@link #getKeyspaceName() configured keyspace}.
	 *
	 * @return the {@link CassandraSessionFactoryBean}.
	 * @throws ClassNotFoundException if an error occurs initializing the initial entity set, see
	 *           {@link #cassandraMapping()}
	 * @see #cluster()
	 * @see #cassandraConverter()
	 * @see #getKeyspaceName()
	 * @see #getSchemaAction()
	 * @see #getStartupScripts()
	 * @see #getShutdownScripts()
	 */
	@Bean
	public CassandraSessionFactoryBean session() throws ClassNotFoundException {

		CassandraSessionFactoryBean session = new CassandraSessionFactoryBean();

		session.setCluster(cluster().getObject());
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
	 * @throws ClassNotFoundException if an error occurs initializing the initial entity set, see
	 *           {@link #cassandraMapping()}
	 * @since 2.0
	 */
	@Bean
	public SessionFactory sessionFactory() throws ClassNotFoundException {
		return new DefaultSessionFactory(session().getObject());
	}

	/**
	 * Creates a {@link CassandraConverter} using the configured {@link #cassandraMapping()}. Will apply all specified
	 * {@link #customConversions()}.
	 *
	 * @return {@link CassandraConverter} used to convert Java and Cassandra value types during the mapping process.
	 * @throws ClassNotFoundException if an error occurs initializing the initial entity set, see
	 *           {@link #cassandraMapping()}
	 * @see #cassandraMapping()
	 * @see #customConversions()
	 */
	@Bean
	public CassandraConverter cassandraConverter() throws ClassNotFoundException {

		MappingCassandraConverter mappingCassandraConverter = new MappingCassandraConverter(cassandraMapping());

		mappingCassandraConverter.setCustomConversions(customConversions());

		return mappingCassandraConverter;
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
	 * Return the {@link MappingContext} instance to map Entities to properties.
	 *
	 * @throws ClassNotFoundException if the Cassandra Entity class type identified by name cannot be found during the
	 *           scan.
	 * @see CassandraMappingContext
	 */
	@Bean
	public CassandraMappingContext cassandraMapping() throws ClassNotFoundException {

		BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();

		mappingContext.setBeanClassLoader(beanClassLoader);
		mappingContext.setInitialEntitySet(CassandraEntityClassScanner.scan(getEntityBasePackages()));

		CustomConversions customConversions = customConversions();

		mappingContext.setCustomConversions(customConversions);
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
		mappingContext.setUserTypeResolver(new SimpleUserTypeResolver(cluster().getObject(), getKeyspaceName()));

		return mappingContext;
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
