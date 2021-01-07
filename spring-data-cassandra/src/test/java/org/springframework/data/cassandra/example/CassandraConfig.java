/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.example;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.config.CqlSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import com.datastax.oss.driver.api.core.CqlSession;

// tag::class[]
@Configuration
@EnableCassandraRepositories(basePackages = { "org.springframework.data.cassandra.example" })
public class CassandraConfig {

	@Bean
	public CqlSessionFactoryBean session() {

		CqlSessionFactoryBean session = new CqlSessionFactoryBean();
		session.setContactPoints("localhost");
		session.setKeyspaceName("mykeyspace");

		return session;
	}

	@Bean
	public SessionFactoryFactoryBean sessionFactory(CqlSession session, CassandraConverter converter) {

		SessionFactoryFactoryBean sessionFactory = new SessionFactoryFactoryBean();
		sessionFactory.setSession(session);
		sessionFactory.setConverter(converter);
		sessionFactory.setSchemaAction(SchemaAction.NONE);

		return sessionFactory;
	}

	@Bean
	public CassandraMappingContext mappingContext(CqlSession cqlSession) {

		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.setUserTypeResolver(new SimpleUserTypeResolver(cqlSession));

		return mappingContext;
	}

	@Bean
	public CassandraConverter converter(CassandraMappingContext mappingContext) {
		return new MappingCassandraConverter(mappingContext);
	}

	@Bean
	public CassandraOperations cassandraTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
		return new CassandraTemplate(sessionFactory, converter);
	}
}
// end::class[]
