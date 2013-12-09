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
package org.springframework.data.cassandra.config;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.cassandra.core.CassandraTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.SpringDataKeyspace;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Session;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
@Configuration
public abstract class AbstractSpringDataCassandraConfiguration extends AbstractCassandraConfiguration implements
		BeanClassLoaderAware {

	private ClassLoader beanClassLoader;

	/**
	 * Creates a {@link SpringDataKeyspace} to be used by the {@link CassandraTemplate}. Will use the {@link Session}
	 * instance configured in {@link #session()} and {@link CassandraConverter} configured in {@link #converter()}.
	 * 
	 * @see #cluster()
	 * @see #Keyspace()
	 * @return
	 * @throws Exception
	 */
	@Bean
	public SpringDataKeyspace keyspace() throws Exception {
		return new SpringDataKeyspace(getKeyspace(), session(), converter());
	}

	/**
	 * Return the base package to scan for mapped {@link Table}s. Will return the package name of the configuration class'
	 * (the concrete class, not this one here) by default. So if you have a {@code com.acme.AppConfig} extending
	 * {@link AbstractSpringDataCassandraConfiguration} the base package will be considered {@code com.acme} unless the
	 * method is overriden to implement alternate behaviour.
	 * 
	 * @return the base package to scan for mapped {@link Table} classes or {@literal null} to not enable scanning for
	 *         entities.
	 */
	protected String getMappingBasePackage() {
		return getClass().getPackage().getName();
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 * 
	 * @return
	 * @throws Exception
	 */
	@Bean
	public CassandraAdminOperations cassandraAdminTemplate() throws Exception {
		return new CassandraAdminTemplate(keyspace());
	}

	/**
	 * Return the {@link MappingContext} instance to map Entities to properties.
	 * 
	 * @return
	 * @throws Exception
	 */
	@Bean
	public MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext() {
		return new CassandraMappingContext();
	}

	/**
	 * Return the {@link CassandraConverter} instance to convert Rows to Objects, Objects to BuiltStatements
	 * 
	 * @return
	 * @throws Exception
	 */
	@Bean
	public CassandraConverter converter() {
		MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext());
		converter.setBeanClassLoader(beanClassLoader);
		return converter;
	}

	/**
	 * Scans the mapping base package for classes annotated with {@link Table}.
	 * 
	 * @see #getMappingBasePackage()
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {

		String basePackage = getMappingBasePackage();
		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
					false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Table.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(),
						AbstractSpringDataCassandraConfiguration.class.getClassLoader()));
			}
		}

		return initialEntitySet;
	}

	/**
	 * Bean ClassLoader Aware for CassandraTemplate/CassandraAdminTemplate
	 */

	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

}
