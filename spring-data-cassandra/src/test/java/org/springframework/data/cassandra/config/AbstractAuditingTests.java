/*
 * Copyright 2019-2021 the original author or authors.
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

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import org.springframework.context.ApplicationContext;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Abstract integration tests for the auditing support.
 *
 * @author Mark Paluch
 */
abstract class AbstractAuditingTests {

	@Test // DATACASS-4
	void enablesAuditingAndSetsPropertiesAccordingly() throws Exception {

		ApplicationContext context = getApplicationContext();

		CassandraMappingContext mappingContext = context.getBean(CassandraMappingContext.class);
		mappingContext.getPersistentEntity(Entity.class);

		EntityCallbacks callbacks = EntityCallbacks.create(context);

		Entity entity = new Entity();
		entity = callbacks.callback(BeforeConvertCallback.class, entity, CqlIdentifier.fromCql("entity"));

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isEqualTo(entity.created);

		Thread.sleep(10);
		entity.id = 1L;

		entity = callbacks.callback(BeforeConvertCallback.class, entity, CqlIdentifier.fromCql("entity"));

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isAfter(entity.created);
	}

	@Test // DATACASS-4
	void enablesReactiveAuditingAndSetsPropertiesAccordingly() throws Exception {

		ApplicationContext context = getApplicationContext();

		CassandraMappingContext mappingContext = context.getBean(CassandraMappingContext.class);
		mappingContext.getPersistentEntity(Entity.class);

		ReactiveEntityCallbacks callbacks = ReactiveEntityCallbacks.create(context);

		Entity entity = new Entity();
		entity = callbacks.callback(BeforeConvertCallback.class, entity, CqlIdentifier.fromCql("entity")).block();

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isEqualTo(entity.created);

		Thread.sleep(10);
		entity.id = 1L;

		entity = callbacks.callback(BeforeConvertCallback.class, entity, CqlIdentifier.fromCql("entity")).block();

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isAfter(entity.created);
	}

	protected abstract ApplicationContext getApplicationContext();

	@Table
	private
	class Entity {

		@Id private Long id;
		@CreatedDate private LocalDateTime created;
		private LocalDateTime modified;

		@LastModifiedDate
		public LocalDateTime getModified() {
			return modified;
		}
	}
}
