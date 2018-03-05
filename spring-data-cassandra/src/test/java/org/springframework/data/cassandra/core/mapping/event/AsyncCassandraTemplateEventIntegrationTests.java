/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping.event;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.query.Criteria.*;
import static org.springframework.data.cassandra.core.query.Query.*;

import java.util.List;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.AsyncCassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.User;

import com.datastax.driver.core.Statement;

/**
 * Integration test for mapping events via {@link AsyncCassandraTemplate}.
 *
 * @author Mark Paluch
 */
public class AsyncCassandraTemplateEventIntegrationTests extends EventListenerIntegrationTestSupport {

	AsyncCassandraTemplate template;

	@Before
	public void setUp() {

		template = new AsyncCassandraTemplate(session);
		template.setApplicationEventPublisher(getApplicationEventPublisher());

		super.setUp();
	}

	@Test // DATACASS-106
	public void selectWithCallbackShouldEmitEvents() {

		getUninterruptibly(template.select("SELECT * FROM users;", it -> {}, User.class));

		assertThat(getListener().getAfterLoad()).extracting(CassandraMappingEvent::getTableName)
				.contains(CqlIdentifier.of("users"));
		assertThat(getListener().getAfterConvert()).extracting(CassandraMappingEvent::getSource).containsOnly(firstUser);
	}

	@Test // DATACASS-106
	public void selectByQueryWithCallbackShouldEmitEvents() {

		getUninterruptibly(template.select(query(where("id").is(firstUser.getId())), it -> {}, User.class));

		assertThat(getListener().getAfterLoad()).extracting(CassandraMappingEvent::getTableName)
				.contains(CqlIdentifier.of("users"));
		assertThat(getListener().getAfterConvert()).extracting(CassandraMappingEvent::getSource).containsOnly(firstUser);
	}

	@Test // DATACASS-106
	public void sliceShouldEmitEvents() {

		getUninterruptibly(template.slice(Query.empty(), User.class));

		assertThat(getListener().getAfterLoad()).extracting(CassandraMappingEvent::getTableName)
				.contains(CqlIdentifier.of("users"));
		assertThat(getListener().getAfterConvert()).extracting(CassandraMappingEvent::getSource).containsOnly(firstUser);
	}

	@Override
	public CassandraOperationsAccessor getAccessor() {

		return new CassandraOperationsAccessor() {

			@Override
			public void insert(Object entity) {
				getUninterruptibly(template.insert(entity));
			}

			@Override
			public void update(Object entity) {
				getUninterruptibly(template.update(entity));
			}

			@Override
			public void delete(Object entity) {
				getUninterruptibly(template.delete(entity));
			}

			@Override
			public void deleteById(Object id, Class<?> entityClass) {
				getUninterruptibly(template.deleteById(id, entityClass));
			}

			@Override
			public void delete(Query query, Class<?> entityClass) {
				getUninterruptibly(template.delete(query, entityClass));
			}

			@Override
			public void truncate(Class<?> entityClass) {
				getUninterruptibly(template.truncate(entityClass));
			}

			@Override
			public <T> T selectOneById(String id, Class<T> entityClass) {
				return getUninterruptibly(template.selectOneById(id, entityClass));
			}

			@Override
			public <T> List<T> select(Query query, Class<T> entityClass) {
				return getUninterruptibly(template.select(query, entityClass));
			}

			@Override
			public <T> List<T> select(Statement statement, Class<T> entityClass) {
				return getUninterruptibly(template.select(statement, entityClass));
			}
		};
	}

	private static <T> T getUninterruptibly(Future<T> future) {

		try {
			return future.get();
		} catch (Exception cause) {
			throw new IllegalStateException(cause);
		}
	}
}
