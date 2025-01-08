/*
 * Copyright 2020-2025 the original author or authors.
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
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;

/**
 * Integration tests for registering both, imperative and reactive auditing handlers.
 *
 * @author Mark Paluch
 */
class CassandraAuditingRegistrarIntegrationTests {

	@Test
	void shouldRegisterPersistentEntitiesOnce() {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.setAllowBeanDefinitionOverriding(false);
		context.register(MyConfiguration.class);

		context.refresh();

		assertThat(context.getBean(IsNewAwareAuditingHandler.class)).isNotNull();
		assertThat(context.getBean(ReactiveIsNewAwareAuditingHandler.class)).isNotNull();

		context.stop();
	}

	@EnableCassandraAuditing
	@EnableReactiveCassandraAuditing
	static class MyConfiguration {

		@Bean
		CassandraConverter cassandraConverter(CassandraMappingContext context) {
			return new MappingCassandraConverter(context);
		}

		@Bean
		CassandraMappingContext cassandraMappingContext() {
			return new CassandraMappingContext();
		}

	}
}
