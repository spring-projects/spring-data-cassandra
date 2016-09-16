/*
 * Copyright 2016 the original author or authors.
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

import org.springframework.cassandra.core.DefaultBridgedReactiveSession;
import org.springframework.cassandra.core.DefaultReactiveSessionFactory;
import org.springframework.cassandra.core.ReactiveCqlTemplate;
import org.springframework.cassandra.core.ReactiveSession;
import org.springframework.cassandra.core.ReactiveSessionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;

import reactor.core.scheduler.Schedulers;

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
	 * @return
	 * @see #session()
	 * @see DefaultBridgedReactiveSession
	 */
	@Bean
	public ReactiveSession reactiveSession() throws Exception {
		return new DefaultBridgedReactiveSession(session().getObject(), Schedulers.elastic());
	}

	/**
	 * Creates a {@link ReactiveSessionFactory} to be used by the {@link ReactiveCassandraTemplate}. Will use the
	 * {@link ReactiveSession} instance configured in {@link #reactiveSession()}.
	 * 
	 * @return
	 * @see #reactiveSession()
	 * @see #reactiveCassandraTemplate()
	 */
	@Bean
	public ReactiveSessionFactory reactiveSessionFactory() throws Exception {
		return new DefaultReactiveSessionFactory(reactiveSession());
	}

	/**
	 * Creates a {@link CassandraAdminTemplate}.
	 * 
	 * @return
	 * @see #reactiveSessionFactory()
	 * @see #cassandraConverter()
	 */
	@Bean
	public ReactiveCassandraTemplate reactiveCassandraTemplate() throws Exception {
		return new ReactiveCassandraTemplate(reactiveSessionFactory(), cassandraConverter());
	}

	/**
	 * Creates a {@link ReactiveCqlTemplate} using the configured {@link ReactiveSessionFactory}.
	 * 
	 * @return
	 * @see #reactiveSessionFactory()
	 */
	@Bean
	public ReactiveCqlTemplate reactiveCqlTemplate() throws Exception {
		return new ReactiveCqlTemplate(reactiveSessionFactory());
	}
}
