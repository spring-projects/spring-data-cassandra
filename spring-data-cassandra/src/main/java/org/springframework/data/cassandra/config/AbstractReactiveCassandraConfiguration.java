/*
 * Copyright 2016-2018 the original author or authors.
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
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;

/**
 * Extension to {@link AbstractCassandraConfiguration} providing Spring Data Cassandra configuration for Spring Data's
 * Reactive Cassandra support using JavaConfig.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class AbstractReactiveCassandraConfiguration extends AbstractCassandraConfiguration {

	/**
	 * Creates a {@link ReactiveSession} object. This wraps a {@link com.datastax.driver.core.Session} to expose Cassandra
	 * access in a reactive style.
	 *
	 * @return the {@link ReactiveSession}.
	 * @see #session()
	 * @see DefaultBridgedReactiveSession
	 */
	@Bean
	public ReactiveSession reactiveSession() {
		return new DefaultBridgedReactiveSession(getRequiredSession());
	}

	/**
	 * Creates a {@link ReactiveSessionFactory} to be used by the {@link ReactiveCassandraTemplate}. Uses the
	 * {@link ReactiveSession} instance configured in {@link #reactiveSession()}.
	 *
	 * @return the {@link ReactiveSessionFactory}.
	 * @see #reactiveSession()
	 * @see #reactiveCassandraTemplate()
	 */
	@Bean
	public ReactiveSessionFactory reactiveSessionFactory() {
		return new DefaultReactiveSessionFactory(reactiveSession());
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 *
	 * @return the {@link ReactiveCassandraOperations}.
	 * @see #reactiveSessionFactory()
	 * @see #cassandraConverter()
	 */
	@Bean
	public ReactiveCassandraOperations reactiveCassandraTemplate() {
		return new ReactiveCassandraTemplate(reactiveSessionFactory(), cassandraConverter());
	}

	/**
	 * Creates a {@link ReactiveCqlTemplate} using the configured {@link ReactiveSessionFactory}.
	 *
	 * @return the {@link ReactiveCqlOperations}.
	 * @see #reactiveSessionFactory()
	 */
	@Bean
	public ReactiveCqlOperations reactiveCqlTemplate() {
		return new ReactiveCqlTemplate(reactiveSessionFactory());
	}
}
