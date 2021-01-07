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
package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.cassandra.support.CassandraConnectionProperties;
import org.springframework.data.cassandra.test.util.CassandraExtension;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;

/**
 * Unit tests for {@link AbstractSessionConfiguration}.
 *
 * @author Mark Paluch
 */
@ExtendWith(CassandraExtension.class)
class AbstractSessionConfigurationIntegrationTests {

	@Test
	void shouldApplyDriverConfigLoaderBuilderConfigurer() {

		MySessionConfiguration configuration = new MySessionConfiguration();
		CqlSessionFactoryBean bean = configuration.cassandraSession();
		bean.afterPropertiesSet();
		CqlSession session = bean.getObject();

		assertThat(session.getContext().getConfig().getProfile("foo")).isNotNull();
		assertThat(session.getContext().getConfig().getProfiles()).doesNotContainKeys("bar");
	}

	@Test
	void shouldApplyConfigurationFile() {

		MySessionConfiguration configuration = new MySessionConfiguration();
		CqlSessionFactoryBean bean = configuration.cassandraSession();
		bean.afterPropertiesSet();
		CqlSession session = bean.getObject();

		assertThat(session.getContext().getConfig().getProfile("oltp")).isNotNull();
		assertThat(session.getContext().getConfig().getProfiles()).doesNotContainKeys("bar");
	}

	static class MySessionConfiguration extends AbstractSessionConfiguration {

		@Override
		protected String getKeyspaceName() {
			return "system";
		}

		@Override
		protected int getPort() {
			return new CassandraConnectionProperties().getCassandraPort();
		}

		@Nullable
		@Override
		protected DriverConfigLoaderBuilderConfigurer getDriverConfigLoaderBuilderConfigurer() {
			return it -> {
				it.startProfile("foo").withString(DefaultDriverOption.SESSION_NAME, "hello-world").endProfile();
			};
		}

		@Nullable
		@Override
		protected Resource getDriverConfigurationResource() {
			return new ClassPathResource("application.conf");
		}
	}

}
