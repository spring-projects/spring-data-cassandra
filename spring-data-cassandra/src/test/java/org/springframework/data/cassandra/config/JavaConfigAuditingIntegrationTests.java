/*
 * Copyright 2019-present the original author or authors.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Unit tests for auditing enabled using Java config.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig
class JavaConfigAuditingIntegrationTests extends AbstractAuditingTests {

	@Autowired ApplicationContext context;

	@Override
	public ApplicationContext getApplicationContext() {
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
		public CqlSessionFactoryBean cassandraSession() {

			DriverContext driverContext = mock(DriverContext.class);
			when(driverContext.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);

			CqlSession session = mock(CqlSession.class);
			when(session.getContext()).thenReturn(driverContext);

			CqlSessionFactoryBean sessionFactoryBean = mock(CqlSessionFactoryBean.class);
			when(sessionFactoryBean.getObject()).thenReturn(session);
			when(sessionFactoryBean.getObjectType()).thenAnswer(invocation -> CqlSession.class);

			return sessionFactoryBean;
		}
	}
}
