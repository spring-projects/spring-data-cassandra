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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.factory.BeanClassLoaderAware;
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
@SuppressWarnings("unused")
public abstract class AbstractCassandraConfiguration extends AbstractSessionConfiguration
		implements BeanClassLoaderAware {

	private @Nullable ClassLoader beanClassLoader;

	/**
	 * Creates a {@link CassandraConverter} using the configured {@link #cassandraMapping()}.
	 *
	 * Will apply all specified {@link #customConversions()}.
	 *
	 * @return {@link CassandraConverter} used to convert Java and Cassandra value types during the mapping process.
	 * @see #cassandraMapping()
	 * @see #customConversions()
	 */
	@Bean
	public CassandraConverter cassandraConverter() {

		UserTypeResolver userTypeResolver =
			new SimpleUserTypeResolver(getRequiredSession(), CqlIdentifier.fromCql(getKeyspaceName()));

		MappingCassandraConverter converter =
			new MappingCassandraConverter(requireBeanOfType(CassandraMappingContext.class));

		converter.setCodecRegistry(getRequiredSession().getContext().getCodecRegistry());
		converter.setUserTypeResolver(userTypeResolver);
		converter.setCustomConversions(requireBeanOfType(CassandraCustomConversions.class));

		return converter;
	}

	/**
	 * Return the {@link MappingContext} instance to map Entities to {@link Object Java Objects}.
	 *
	 * @throws ClassNotFoundException if the Cassandra Entity class type identified by name
	 * cannot be found during the scan.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraMappingContext
	 */
	@Bean
	public CassandraMappingContext cassandraMapping() throws ClassNotFoundException {

		UserTypeResolver userTypeResolver =
			new SimpleUserTypeResolver(getRequiredSession(), CqlIdentifier.fromCql(getKeyspaceName()));

		CassandraMappingContext mappingContext =
			new CassandraMappingContext(userTypeResolver, SimpleTupleTypeFactory.DEFAULT);

		CustomConversions customConversions = requireBeanOfType(CassandraCustomConversions.class);

		getBeanClassLoader().ifPresent(mappingContext::setBeanClassLoader);

		mappingContext.setCodecRegistry(getRequiredSession().getContext().getCodecRegistry());
		mappingContext.setCustomConversions(customConversions);
		mappingContext.setInitialEntitySet(getInitialEntitySet());
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		return mappingContext;
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
	public SessionFactoryFactoryBean cassandraSessionFactory(CqlSession cqlSession) {

		SessionFactoryFactoryBean bean = new SessionFactoryFactoryBean();

		// Initialize the CqlSession reference first since it is required, or must not be null!
		bean.setSession(cqlSession);

		bean.setConverter(requireBeanOfType(CassandraConverter.class));
		bean.setKeyspaceCleaner(keyspaceCleaner());
		bean.setKeyspacePopulator(keyspacePopulator());
		bean.setSchemaAction(getSchemaAction());

		return bean;
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 */
	@Bean
	public CassandraAdminTemplate cassandraTemplate() {
		return new CassandraAdminTemplate(getRequiredSessionFactory(), requireBeanOfType(CassandraConverter.class));
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
	public CassandraCustomConversions customConversions() {
		return new CassandraCustomConversions(Collections.emptyList());
	}

	/**
	 * Configures the Java {@link ClassLoader} used to resolve Cassandra application entity {@link Class types}.
	 *
	 * @param classLoader Java {@link ClassLoader} used to resolve Cassandra application entity {@link Class types}; may
	 *          be {@literal null}.
	 * @see java.lang.ClassLoader
	 */
	@Override
	public void setBeanClassLoader(@Nullable ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	/**
	 * Returns the configured Java {@link ClassLoader} used to resolve Cassandra application entity {@link Class types}.
	 *
	 * @return the Java {@link ClassLoader} used to resolve Cassandra application entity {@link Class types}.
	 * @see java.lang.ClassLoader
	 * @see java.util.Optional
	 */
	protected Optional<ClassLoader> getBeanClassLoader() {
		return Optional.ofNullable(this.beanClassLoader);
	}

	/**
	 * Base packages to scan for entities annotated with {@link Table} annotations. By default, returns the package name
	 * of {@literal this} ({@code this.getClass().getPackage().getName()}. This method must never return {@literal null}.
	 */
	public String[] getEntityBasePackages() {
		return new String[] { getClass().getPackage().getName() };
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
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected SessionFactory getRequiredSessionFactory() {
		return requireBeanOfType(SessionFactory.class);
	}

	/**
	 * The {@link SchemaAction} to perform at application startup. Defaults to {@link SchemaAction#NONE}.
	 *
	 * @see org.springframework.data.cassandra.config.SchemaAction
	 */
	public SchemaAction getSchemaAction() {
		return SchemaAction.NONE;
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
