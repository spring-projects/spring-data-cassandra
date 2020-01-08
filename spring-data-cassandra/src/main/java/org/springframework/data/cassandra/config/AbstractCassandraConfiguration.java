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
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.session.init.KeyspacePopulator;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleTupleTypeFactory;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 */
@Configuration
public abstract class AbstractCassandraConfiguration extends AbstractSessionConfiguration
		implements BeanClassLoaderAware, BeanFactoryAware {

	private @Nullable ClassLoader beanClassLoader;
	private @Nullable BeanFactory beanFactory;

	/**
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected SessionFactory getRequiredSessionFactory() {

		Assert.state(beanFactory != null, "BeanFactory not initialized");

		return beanFactory.getBean(SessionFactory.class);
	}

	/**
	 * Creates a {@link SessionFactoryFactoryBean} that provides a {@link SessionFactory}. The lifecycle of
	 * {@link SessionFactoryFactoryBean} initializes the {@link #getSchemaAction() schema} in the
	 * {@link #getKeyspaceName() configured keyspace}.
	 *
	 * @return the {@link SessionFactoryFactoryBean}.
	 * @see #cassandraConverter()
	 * @see #getKeyspaceName()
	 * @see #getSchemaAction()
	 * @see #keyspacePopulator()
	 * @see #keyspaceCleaner()
	 */
	@Bean
	public SessionFactoryFactoryBean sessionFactory(CqlSession cqlSession) {

		SessionFactoryFactoryBean bean = new SessionFactoryFactoryBean();

		bean.setSession(cqlSession);

		bean.setConverter(beanFactory.getBean(CassandraConverter.class));
		bean.setSchemaAction(getSchemaAction());
		bean.setKeyspacePopulator(keyspacePopulator());
		bean.setKeyspaceCleaner(keyspaceCleaner());

		return bean;
	}

	/**
	 * Creates a {@link KeyspacePopulator} to initialize the keyspace.
	 *
	 * @return the {@link KeyspacePopulator} or {@code null} if none configured.
	 * @see org.springframework.data.cassandra.core.cql.session.init.ResourceKeyspacePopulator
	 */
	@Nullable
	protected KeyspacePopulator keyspacePopulator() {
		return null;
	}

	/**
	 * Creates a {@link KeyspacePopulator} to cleanup the keyspace.
	 *
	 * @return the {@link KeyspacePopulator} or {@code null} if none configured.
	 * @see org.springframework.data.cassandra.core.cql.session.init.ResourceKeyspacePopulator
	 */
	@Nullable
	protected KeyspacePopulator keyspaceCleaner() {
		return null;
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

		MappingCassandraConverter mappingCassandraConverter = new MappingCassandraConverter(
				beanFactory.getBean(CassandraMappingContext.class));

		mappingCassandraConverter.setCustomConversions(beanFactory.getBean(CassandraCustomConversions.class));

		return mappingCassandraConverter;
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

		UserTypeResolver userTypeResolver = new SimpleUserTypeResolver(getRequiredSession(),
				CqlIdentifier.fromCql(getKeyspaceName()));

		CassandraMappingContext mappingContext = new CassandraMappingContext(userTypeResolver,
				SimpleTupleTypeFactory.DEFAULT);

		Optional.ofNullable(this.beanClassLoader).ifPresent(mappingContext::setBeanClassLoader);

		mappingContext.setInitialEntitySet(getInitialEntitySet());

		CustomConversions customConversions = beanFactory.getBean(CustomConversions.class);

		mappingContext.setCustomConversions(customConversions);
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
		mappingContext.setCodecRegistry(getRequiredSession().getContext().getCodecRegistry());

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
	 * {@link #getEntityBasePackages()}. Can be overridden by subclasses to skip class path scanning and return a fixed
	 * set of entity classes.
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
	 */
	@Bean
	public CassandraAdminTemplate cassandraTemplate() {
		return new CassandraAdminTemplate(getRequiredSessionFactory(), beanFactory.getBean(CassandraConverter.class));
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		super.setBeanFactory(beanFactory);
	}

	/**
	 * Base packages to scan for entities annotated with {@link Table} annotations. By default, returns the package name
	 * of {@literal this} ({@code this.getClass().getPackage().getName()}. This method must never return {@literal null}.
	 */
	public String[] getEntityBasePackages() {
		return new String[] { getClass().getPackage().getName() };
	}

	/**
	 * The {@link SchemaAction} to perform at startup. Defaults to {@link SchemaAction#NONE}.
	 */
	public SchemaAction getSchemaAction() {
		return SchemaAction.NONE;
	}

	/**
	 * Creates a new {@link ByteArrayResource} given {@code content}.
	 *
	 * @param content the script content.
	 * @return a new {@link ByteArrayResource} for {@code content}.
	 * @since 3.0
	 */
	protected ByteArrayResource scriptOf(String content) {
		return new ByteArrayResource(content.getBytes());
	}
}
