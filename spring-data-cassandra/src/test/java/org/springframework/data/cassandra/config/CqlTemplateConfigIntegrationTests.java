/*
 * Copyright 2017-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.After;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.test.util.CassandraExtension;
import org.springframework.data.cassandra.test.util.IntegrationTestsSupport;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Integration tests for {@link AbstractCqlTemplateConfiguration}.
 *
 * @author Matthews T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 */
class CqlTemplateConfigIntegrationTests extends IntegrationTestsSupport {

	@Configuration
	static class Config extends AbstractCqlTemplateConfiguration {

		@Override
		protected String getKeyspaceName() {
			return "system";
		}

		@Override
		protected int getPort() {
			return CassandraExtension.getResources().getPort();
		}

	}

	private CqlSession session;
	private ConfigurableApplicationContext context;

	@BeforeEach
	void setUp() {

		this.context = new AnnotationConfigApplicationContext(Config.class);
		this.session = context.getBean(CqlSession.class);
	}

	@After
	void tearDown() {
		context.close();
	}

	@Test
	void test() {

		CqlTemplate cqlTemplate = context.getBean(CqlTemplate.class);
		assertThat(cqlTemplate.describeRing()).isNotEmpty();
	}
}
