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

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.User;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Integration test for mapping events via {@link CassandraTemplate}.
 *
 * @author Mark Paluch
 */
class CassandraTemplateEventIntegrationTests extends EventListenerIntegrationTestSupport {

	private CassandraTemplate template;

	@BeforeEach
	void setUp() {

		template = new CassandraTemplate(session);
		template.setApplicationEventPublisher(getApplicationEventPublisher());

		super.setUp();
	}

	@Test // DATACASS-106
	void streamShouldEmitEvents() {

		template.stream("SELECT * FROM users;", User.class).collect(Collectors.toList()); // Just load entire stream.

		assertThat(getListener().getAfterLoad()).extracting(CassandraMappingEvent::getTableName)
				.contains(CqlIdentifier.fromCql("users"));
		assertThat(getListener().getAfterConvert()).extracting(CassandraMappingEvent::getSource).containsOnly(firstUser);
	}

	@Test // DATACASS-106
	void sliceShouldEmitEvents() {

		template.slice(Query.empty(), User.class).getSize(); // Force load entire collection.

		assertThat(getListener().getAfterLoad()).extracting(CassandraMappingEvent::getTableName)
				.contains(CqlIdentifier.fromCql("users"));
		assertThat(getListener().getAfterConvert()).extracting(CassandraMappingEvent::getSource).containsOnly(firstUser);
	}

	@Override
	public CassandraOperationsAccessor getAccessor() {

		return new CassandraOperationsAccessor() {
			@Override
			public void insert(Object entity) {
				template.insert(entity);
			}

			@Override
			public void update(Object entity) {
				template.update(entity);
			}

			@Override
			public void delete(Object entity) {
				template.delete(entity);
			}

			@Override
			public void deleteById(Object id, Class<?> entityClass) {
				template.deleteById(id, entityClass);
			}

			@Override
			public void delete(Query query, Class<?> entityClass) {
				template.delete(query, entityClass);
			}

			@Override
			public void truncate(Class<?> entityClass) {
				template.truncate(entityClass);
			}

			@Override
			public <T> T selectOneById(String id, Class<T> entityClass) {
				return template.selectOneById(id, entityClass);
			}

			@Override
			public <T> List<T> select(Query query, Class<T> entityClass) {
				return template.select(query, entityClass);
			}

			@Override
			public <T> List<T> select(SimpleStatement statement, Class<T> entityClass) {
				return template.select(statement, entityClass);
			}
		};
	}
}
