/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping.event;

import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.data.cassandra.core.query.Query;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Integration test for mapping events via {@link ReactiveCassandraTemplate}.
 *
 * @author Lukasz Antoniak
 * @author Mark Paluch
 */
class ReactiveEventListenerIntegrationTestSupport extends EventListenerIntegrationTestSupport {

	private ReactiveCassandraTemplate template;

	@BeforeEach
	void setUp() {

		template = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(session));
		template.setApplicationEventPublisher(getApplicationEventPublisher());

		super.setUp();
	}

	@Override
	public CassandraOperationsAccessor getAccessor() {
		return new CassandraOperationsAccessor() {
			@Override
			public void insert(Object entity) {
				template.insert(entity).as(StepVerifier::create).expectNextCount(1).verifyComplete();
			}

			@Override
			public void update(Object entity) {
				template.update(entity).as(StepVerifier::create).expectNextCount(1).verifyComplete();
			}

			@Override
			public void delete(Object entity) {
				template.delete(entity).as(StepVerifier::create).expectNextCount(1).verifyComplete();
			}

			@Override
			public void deleteById(Object id, Class<?> entityClass) {
				template.deleteById(id, entityClass).as(StepVerifier::create).expectNextCount(1).verifyComplete();
			}

			@Override
			public void delete(Query query, Class<?> entityClass) {
				template.delete(query, entityClass).as(StepVerifier::create).expectNextCount(1).verifyComplete();
			}

			@Override
			public void truncate(Class<?> entityClass) {
				template.truncate(entityClass).as(StepVerifier::create).verifyComplete();
			}

			@Override
			public <T> T selectOneById(String id, Class<T> entityClass) {

				List<T> result = new CopyOnWriteArrayList<>();
				template.selectOneById(id, entityClass).as(StepVerifier::create).recordWith(() -> result).expectNextCount(1)
						.verifyComplete();

				return result.get(0);
			}

			@Override
			public <T> List<T> select(Query query, Class<T> entityClass) {

				List<T> result = new CopyOnWriteArrayList<>();
				template.select(query, entityClass).as(StepVerifier::create).recordWith(() -> result).expectNextCount(1)
						.verifyComplete();

				return result;
			}

			@Override
			public <T> List<T> select(SimpleStatement statement, Class<T> entityClass) {

				List<T> result = new CopyOnWriteArrayList<>();
				template.select(statement, entityClass).as(StepVerifier::create).recordWith(() -> result).expectNextCount(1)
						.verifyComplete();

				return result;
			}
		};
	}
}
