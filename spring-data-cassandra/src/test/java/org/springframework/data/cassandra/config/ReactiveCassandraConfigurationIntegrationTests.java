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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for {@link AbstractReactiveCassandraConfiguration}.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(classes = IntegrationTestConfig.class)
class ReactiveCassandraConfigurationIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired BeanFactory beanFactory;

	@Test // DATACASS-713
	void shouldContainCassandraSessionBean() {
		assertThat(beanFactory.containsBean(DefaultCqlBeanNames.SESSION)).isTrue();
	}

	@Test // DATACASS-713
	void shouldContainCassandraSessionFactoryBean() {
		assertThat(beanFactory.containsBean(DefaultCqlBeanNames.SESSION_FACTORY)).isTrue();
	}

	@Test // DATACASS-713
	void shouldContainReactiveCassandraSessionBean() {
		assertThat(beanFactory.containsBean("reactiveCassandraSession")).isTrue();
	}

	@Test // DATACASS-713
	void shouldContainReactiveCassandraSessionFactoryBean() {
		assertThat(beanFactory.containsBean("reactiveCassandraSessionFactory")).isTrue();
	}
}
