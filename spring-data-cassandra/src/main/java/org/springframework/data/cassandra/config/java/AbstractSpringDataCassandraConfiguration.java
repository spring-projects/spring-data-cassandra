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
import java.util.Set;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.cassandra.config.java.AbstractClusterConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.CassandraDataSessionFactoryBean;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.mapping.Mapping;
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
	protected Mapping mapping = new Mapping();

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
		bean.setConverter(converter());
		bean.setSchemaAction(getSchemaAction());
		bean.setKeyspaceName(getKeyspaceName());
		bean.setStartupScripts(getStartupScripts());
		bean.setShutdownScripts(getShutdownScripts());

		bean.setEntityClassLoader(beanClassLoader);
		bean.setMapping(mapping);

		return bean;
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 * 
	 * @throws Exception
	 */
	@Bean
	public CassandraAdminOperations cassandraTemplate() throws Exception {
		return new CassandraAdminTemplate(session().getObject(), converter());
	}

	/**
	 * Return the {@link MappingContext} instance to map Entities to properties.
	 * 
	 * @throws ClassNotFoundException
	 */
	@Bean
	public CassandraMappingContext cassandraMappingContext() throws ClassNotFoundException {
		DefaultCassandraMappingContext context = new DefaultCassandraMappingContext();
		context.setInitialEntitySet(getInitialEntitySet());
		return context;
	}

	/**
	 * Return the {@link CassandraConverter} instance to convert Rows to Objects, Objects to BuiltStatements
	 * 
	 * @throws ClassNotFoundException
	 */
	@Bean
	public CassandraConverter converter() throws ClassNotFoundException {
		return new MappingCassandraConverter(cassandraMappingContext());
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
