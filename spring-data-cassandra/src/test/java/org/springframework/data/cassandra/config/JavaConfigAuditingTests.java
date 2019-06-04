/*
 * Copyright 2019 the original author or authors.
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

import static org.mockito.Mockito.*;

import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Unit tests for auditing enabled using Java config.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class JavaConfigAuditingTests extends AbstractAuditingTests {

	@Autowired ApplicationContext context;

	@Override
	protected ApplicationContext getApplicationContext() {
		return context;
	}

	@EnableCassandraAuditing
	@Configuration
	static class TestConfig extends AbstractReactiveCassandraConfiguration {

		@Override
		protected String getKeyspaceName() {
			return "foo";
		}

		@Bean
		public CassandraSessionFactoryBean session() {

			CassandraSessionFactoryBean sessionFactoryBean = mock(CassandraSessionFactoryBean.class);
			Session session = mock(Session.class);
			when(sessionFactoryBean.getObject()).thenReturn(session);

			return sessionFactoryBean;
		}

		@Bean
		public CassandraClusterFactoryBean cluster() {

			CassandraClusterFactoryBean cassandraClusterFactoryBean = mock(CassandraClusterFactoryBean.class);
			Cluster cluster = mock(Cluster.class);
			when(cassandraClusterFactoryBean.getObject()).thenReturn(cluster);

			return cassandraClusterFactoryBean;
		}
	}
}
