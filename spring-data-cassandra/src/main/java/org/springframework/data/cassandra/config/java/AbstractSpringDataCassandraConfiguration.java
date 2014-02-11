/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.cassandra.config.java;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.cassandra.config.java.AbstractClusterConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.CassandraDataSessionFactoryBean;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.config.CassandraMappingContextFactoryBean;
import org.springframework.data.cassandra.config.CassandraMappingConverterFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.mapping.context.MappingContext;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
@Configuration
public abstract class AbstractSpringDataCassandraConfiguration extends AbstractClusterConfiguration implements
		BeanClassLoaderAware {

	protected abstract String getKeyspaceName();

	protected ClassLoader beanClassLoader;

	/**
	 * The {@link SchemaAction} to perform. Defaults to {@link SchemaAction#NONE}.
	 */
	public SchemaAction getSchemaAction() {
		return SchemaAction.NONE;
	}

	/**
	 * The base packages to scan for entities annotated with {@link Table} annotations. By default, returns the package
	 * name of {@literal this} (<code>this.getClass().getPackage().getName()</code>). This method must never return null.
	 */
	public String[] getMappingBasePackages() {
		return new String[] { getClass().getPackage().getName() };
	}

	@Bean
	public CassandraDataSessionFactoryBean session() throws Exception {

		CassandraDataSessionFactoryBean bean = new CassandraDataSessionFactoryBean();

		bean.setCluster(cluster().getObject());
		bean.setConverter(cassandraConverter().getObject());
		bean.setSchemaAction(getSchemaAction());
		bean.setKeyspaceName(getKeyspaceName());
		bean.setStartupScripts(getStartupScripts());
		bean.setShutdownScripts(getShutdownScripts());

		return bean;
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 * 
	 * @throws Exception
	 */
	@Bean
	public CassandraAdminOperations cassandraTemplate() throws Exception {
		return new CassandraAdminTemplate(session().getObject(), cassandraConverter().getObject());
	}

	/**
	 * Return the {@link MappingContext} instance to map Entities to properties.
	 * 
	 * @throws ClassNotFoundException
	 */
	@Bean
	public CassandraMappingContextFactoryBean cassandraMapping() throws ClassNotFoundException {

		CassandraMappingContextFactoryBean bean = new CassandraMappingContextFactoryBean();
		bean.setBasePackages(new HashSet<String>(Arrays.asList(getMappingBasePackages())));
		bean.setEntityClassLoader(beanClassLoader);

		return bean;
	}

	/**
	 * Return the {@link CassandraConverter} instance to convert Rows to Objects, Objects to BuiltStatements
	 */
	@Bean
	public CassandraMappingConverterFactoryBean cassandraConverter() throws Exception {

		CassandraMappingConverterFactoryBean bean = new CassandraMappingConverterFactoryBean();
		bean.setMappingContext(cassandraMapping().getObject());

		return bean;
	}

	/**
	 * Scans the mapping base package for entity classes.
	 * 
	 * @see #getMappingBasePackages()
	 * @see #getEntityScanner()
	 * @return <code>Set&lt;Class&lt;?&gt;&gt;</code> representing the annotated entity classes found.
	 * @throws ClassNotFoundException
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {

		CassandraEntityClassScanner entityScanner = getEntityScanner();
		entityScanner.setEntityBasePackages(Arrays.asList(getMappingBasePackages()));

		return entityScanner.scanForEntityClasses();
	}

	public CassandraEntityClassScanner getEntityScanner() {
		return new CassandraEntityClassScanner();
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}
}
