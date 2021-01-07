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
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.core.cql.session.init.CompositeKeyspacePopulator;
import org.springframework.data.cassandra.core.cql.session.init.ResourceKeyspacePopulator;
import org.springframework.data.cassandra.core.cql.session.init.SessionFactoryInitializer;

// tag::class[]
@Configuration
public class SessionFactoryInitializerConfiguration extends AbstractCassandraConfiguration {

	@Bean
	SessionFactoryInitializer sessionFactoryInitializer(SessionFactory sessionFactory) {

		SessionFactoryInitializer initializer = new SessionFactoryInitializer();
		initializer.setSessionFactory(sessionFactory);

		ResourceKeyspacePopulator populator1 = new ResourceKeyspacePopulator();
		populator1.setSeparator(";");
		populator1.setScripts(new ClassPathResource("com/myapp/cql/db-schema.cql"));

		ResourceKeyspacePopulator populator2 = new ResourceKeyspacePopulator();
		populator2.setSeparator("@@");
		populator2.setScripts(new ClassPathResource("classpath:com/myapp/cql/db-test-data-1.cql"), //
				new ClassPathResource("classpath:com/myapp/cql/db-test-data-2.cql"));

		initializer.setKeyspacePopulator(new CompositeKeyspacePopulator(populator1, populator2));

		return initializer;
	}

	// ...
	// end::class[]

	@Override
	protected String getKeyspaceName() {
		return null;
	}
	// tag::class[]
}
// end::class[]
