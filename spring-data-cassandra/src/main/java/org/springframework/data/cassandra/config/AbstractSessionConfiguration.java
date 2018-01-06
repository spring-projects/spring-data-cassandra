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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * Spring {@link @Configuration} class used to configure a Cassandra client application
 * {@link com.datastax.driver.core.Session} connected to a Cassandra {@link com.datastax.driver.core.Cluster}. Enables a
 * Cassandra Keyspace to be specified along with the ability to execute arbitrary CQL on startup as well as shutdown.
 *
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 * @see AbstractClusterConfiguration
 * @see org.springframework.context.annotation.Configuration
 */
@Configuration
public abstract class AbstractSessionConfiguration extends AbstractClusterConfiguration {

	/**
	 * Returns the initialized {@link Session} instance.
	 *
	 * @return the {@link Session}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected Session getRequiredSession() {

		CassandraCqlSessionFactoryBean factoryBean = session();
		Assert.state(factoryBean.getObject() != null, "Session factory not initialized");

		return factoryBean.getObject();
	}

	/**
	 * Creates a {@link CassandraCqlSessionFactoryBean} that provides a Cassandra
	 * {@link com.datastax.driver.core.Session}.
	 *
	 * @return the {@link CassandraCqlSessionFactoryBean}.
	 * @see #cluster()
	 * @see #getKeyspaceName()
	 */
	@Bean
	public CassandraCqlSessionFactoryBean session() {

		CassandraCqlSessionFactoryBean bean = new CassandraCqlSessionFactoryBean();

		bean.setCluster(getRequiredCluster());
		bean.setKeyspaceName(getKeyspaceName());

		return bean;
	}

	/**
	 * Creates a {@link DefaultSessionFactory} using the configured {@link #session()} to be used with
	 * {@link CqlTemplate}.
	 *
	 * @return {@link SessionFactory} used to initialize the Template API.
	 * @since 2.0
	 */
	@Bean
	public SessionFactory sessionFactory() {
		return new DefaultSessionFactory(getRequiredSession());
	}

	/**
	 * Return the name of the keyspace to connect to.
	 *
	 * @return must not be {@literal null}.
	 */
	protected abstract String getKeyspaceName();
}
