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

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.domain.ReactiveAuditorAware;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.test.context.ContextConfiguration;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Abstract integration tests for the reactive auditing support.
 *
 * @author Mark Paluch
 */
@ContextConfiguration
class ReactiveAuditingTests {

	@Autowired ApplicationContext context;

	@EnableReactiveCassandraAuditing
	@Configuration
	static class TestConfig {

		@Bean
		public CassandraMappingContext mappingContext() {
			return new CassandraMappingContext();
		}

		@Bean
		public CassandraConverter cassandraConverter(CassandraMappingContext mappingContext) {
			return new MappingCassandraConverter(mappingContext);
		}

		@Bean
		public ReactiveAuditorAware<String> auditorAware() {
			return () -> Mono.just("Walter");
		}
	}

	@Test // DATACASS-784
	void enablesAuditingAndSetsPropertiesAccordingly() {

		CassandraMappingContext mappingContext = context.getBean(CassandraMappingContext.class);
		mappingContext.getPersistentEntity(Entity.class);

		ReactiveEntityCallbacks callbacks = ReactiveEntityCallbacks.create(context);

		Entity entity = new Entity();
		callbacks.callback(ReactiveBeforeConvertCallback.class, entity, CqlIdentifier.fromCql("entity"))
				.as(StepVerifier::create).consumeNextWith(actual -> {

					assertThat(actual.created).isNotNull();
					assertThat(actual.createdBy).isEqualTo("Walter");
				}).verifyComplete();
	}

	@Table
	private
	class Entity {

		@Id Long id;
		@CreatedDate private LocalDateTime created;
		@CreatedBy private String createdBy;
		private LocalDateTime modified;

		@LastModifiedDate
		public LocalDateTime getModified() {
			return modified;
		}
	}
}
