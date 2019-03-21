/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config.java;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.cassandra.config.java.AbstractClusterConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.mapping.context.MappingContext;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 */
@Configuration
public abstract class AbstractCassandraConfiguration extends AbstractClusterConfiguration
		implements BeanClassLoaderAware {

	protected ClassLoader beanClassLoader;

	@Bean
	public CassandraSessionFactoryBean session() throws Exception {

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
	 * Return the {@link MappingContext} instance to map Entities to properties.
	 *
	 * @throws ClassNotFoundException
	 */
	@Bean
	public CassandraMappingContext cassandraMapping() throws ClassNotFoundException {

		BasicCassandraMappingContext mappingContext = new BasicCassandraMappingContext();

		mappingContext.setBeanClassLoader(beanClassLoader);
		mappingContext.setInitialEntitySet(CassandraEntityClassScanner.scan(getEntityBasePackages()));

		return mappingContext;
	}

	/**
	 * Return the {@link CassandraConverter} instance to convert Rows to Objects, Objects to BuiltStatements
	 */
	@Bean
	public CassandraConverter cassandraConverter() throws Exception {
		return new MappingCassandraConverter(cassandraMapping());
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 *
	 * @throws Exception if the {@link com.datastax.driver.core.Session} could not be obtained.
	 */
	@Bean
	public CassandraAdminOperations cassandraTemplate() throws Exception {
		return new CassandraAdminTemplate(session().getObject(), cassandraConverter());
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	/**
	 * Base packages to scan for entities annotated with {@link Table} annotations. By default, returns the package
	 * name of {@literal this} (<code>this.getClass().getPackage().getName()</code>).
	 *
	 * This method must never return null.
	 */
	public String[] getEntityBasePackages() {
		return new String[] { getClass().getPackage().getName() };
	}

	protected abstract String getKeyspaceName();

	/**
	 * The {@link SchemaAction} to perform at startup.  Defaults to {@link SchemaAction#NONE}.
	 */
	public SchemaAction getSchemaAction() {
		return SchemaAction.NONE;
	}
}
