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

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.DefaultCassandraMappingContext;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

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
	 * The base package to scan for entities annotated with {@link Table} annotations. By default, returns the package
	 * name of {@literal this} (<code>this.getClass().getPackage().getName()</code>).
	 */
	protected String getMappingBasePackage() {
		return getClass().getPackage().getName();
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 * 
	 * @throws Exception
	 */
	@Bean
	public CassandraAdminOperations adminTemplate() throws Exception {
		return new CassandraAdminTemplate(session().getObject());
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
		MappingCassandraConverter converter = new MappingCassandraConverter(cassandraMappingContext());
		converter.setBeanClassLoader(beanClassLoader);
		return converter;
	}

	/**
	 * Scans the mapping base package for entity classes annotated with {@link Table} or {@link Persistent}.
	 * 
	 * @see #getMappingBasePackage()
	 * @return <code>Set&lt;Class&lt;?&gt;&gt;</code> representing the annotated entity classes found.
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

			// TODO: figure out which ClassLoader to use here
			ClassLoader classLoader = getClass().getClassLoader();

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(), classLoader));
			}
		}

		return initialEntitySet;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}
}
