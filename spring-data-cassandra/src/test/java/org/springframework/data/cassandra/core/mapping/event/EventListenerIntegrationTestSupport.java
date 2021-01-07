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
import static org.springframework.data.cassandra.core.query.Criteria.*;
import static org.springframework.data.cassandra.core.query.Query.*;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Integration tests for lifecycle events.
 *
 * @author Lukasz Antoniak
 * @author Mark Paluch
 */
public abstract class EventListenerIntegrationTestSupport extends AbstractKeyspaceCreatingIntegrationTests {

	private CaptureEventListener listener = new CaptureEventListener();
	User firstUser = null;

	@BeforeEach
	void setUp() {

		CassandraTemplate setup = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(User.class, setup);
		SchemaTestUtils.truncate(User.class, setup);

		firstUser = new User("id-1", "Johny", "Bravo");
		setup.insert(firstUser);

		setup.setApplicationEventPublisher(getApplicationEventPublisher());
		listener.clear();
	}

	CaptureEventListener getListener() {
		return listener;
	}

	ApplicationEventPublisher getApplicationEventPublisher() {
		return it -> listener.onApplicationEvent((CassandraMappingEvent) it);
	}

	protected abstract CassandraOperationsAccessor getAccessor();

	@Test // DATACASS-106
	void selectByIdShouldEmitLoadEvents() {

		User loaded = getAccessor().selectOneById(firstUser.getId(), User.class);

		assertThat(listener.getAfterLoad()).extracting(CassandraMappingEvent::getTableName)
				.containsOnly(CqlIdentifier.fromCql("users"));
		assertThat(listener.getAfterConvert()).extracting(CassandraMappingEvent::getSource).containsOnly(loaded);
	}

	@Test // DATACASS-106
	void selectByQueryShouldEmitLoadEvents() {

		List<User> loaded = getAccessor().select(query(where("id").is(firstUser.getId())), User.class);

		assertThat(listener.getAfterLoad()).extracting(CassandraMappingEvent::getTableName)
				.containsOnly(CqlIdentifier.fromCql("users"));
		assertThat(listener.getAfterConvert()).extracting(CassandraMappingEvent::getSource).containsOnly(loaded.get(0));
	}

	@Test // DATACASS-106
	void selectByStatementShouldEmitLoadEvents() {

		List<User> loaded = getAccessor().select(SimpleStatement.newInstance("SELECT * FROM users"), User.class);

		assertThat(listener.getAfterLoad()).extracting(CassandraMappingEvent::getTableName)
				.containsOnly(CqlIdentifier.fromCql("users"));
		assertThat(listener.getAfterConvert()).extracting(CassandraMappingEvent::getSource).containsOnly(loaded.get(0));
	}

	@Test // DATACASS-106
	void insertShouldEmitEvents() {

		User user = new User("id-2", "Lukasz", "Antoniak");
		getAccessor().insert(user);

		assertThat(listener.getBeforeSave()).extracting(CassandraMappingEvent::getSource).containsOnly(user);
		assertThat(listener.getAfterSave()).extracting(CassandraMappingEvent::getSource).containsOnly(user);
	}

	@Test // DATACASS-106
	void updateShouldEmitEvents() {

		firstUser.setLastname("Wayne");
		getAccessor().update(firstUser);

		assertThat(listener.getBeforeSave()).extracting(CassandraMappingEvent::getSource).containsOnly(firstUser);
		assertThat(listener.getAfterSave()).extracting(CassandraMappingEvent::getSource).containsOnly(firstUser);
	}

	@Test // DATACASS-106
	void deleteShouldEmitEvents() {

		getAccessor().delete(firstUser);

		assertThat(listener.getBeforeDelete()).extracting(CassandraMappingEvent::getTableName)
				.containsExactly(CqlIdentifier.fromCql("users"));
		assertThat(listener.getAfterDelete()).extracting(CassandraMappingEvent::getTableName)
				.containsExactly(CqlIdentifier.fromCql("users"));
	}

	@Test // DATACASS-106
	void deleteByIdShouldEmitEvents() {

		getAccessor().deleteById(firstUser.getId(), User.class);

		assertThat(listener.getBeforeDelete()).extracting(CassandraMappingEvent::getTableName)
				.containsExactly(CqlIdentifier.fromCql("users"));
		assertThat(listener.getAfterDelete()).extracting(CassandraMappingEvent::getTableName)
				.containsExactly(CqlIdentifier.fromCql("users"));
	}

	@Test // DATACASS-106
	void deleteByQueryShouldEmitEvents() {

		getAccessor().delete(query(where("id").is(firstUser.getId())), User.class);

		assertThat(listener.getBeforeDelete()).extracting(CassandraMappingEvent::getTableName)
				.containsExactly(CqlIdentifier.fromCql("users"));
		assertThat(listener.getAfterDelete()).extracting(CassandraMappingEvent::getTableName)
				.containsExactly(CqlIdentifier.fromCql("users"));
	}

	@Test // DATACASS-106
	void truncateShouldEmitEvents() {

		getAccessor().truncate(User.class);

		assertThat(listener.getBeforeDelete()).extracting(CassandraMappingEvent::getTableName)
				.containsExactly(CqlIdentifier.fromCql("users"));
		assertThat(listener.getAfterDelete()).extracting(CassandraMappingEvent::getTableName)
				.containsExactly(CqlIdentifier.fromCql("users"));
	}

	@Test // DATACASS-106
	void shouldEmitMultipleEvents() {

		User user = new User("id-2", "Lukasz", "Antoniak");
		getAccessor().insert(user);

		assertThat(listener.getBeforeSave()).hasSize(1);
		assertThat(listener.getAfterSave()).hasSize(1);
		listener.clear();

		user.setFirstname("Robert");
		getAccessor().update(user);

		assertThat(listener.getBeforeSave()).hasSize(1);
		assertThat(listener.getAfterSave()).hasSize(1);
	}

	static class CaptureEventListener extends AbstractCassandraEventListener<User> {

		private final List<CassandraMappingEvent<?>> events = new CopyOnWriteArrayList<>();

		@Override
		public void onBeforeSave(BeforeSaveEvent<User> event) {
			events.add(event);
		}

		@Override
		public void onAfterSave(AfterSaveEvent<User> event) {
			events.add(event);
		}

		@Override
		public void onBeforeDelete(BeforeDeleteEvent<User> event) {
			events.add(event);
		}

		@Override
		public void onAfterDelete(AfterDeleteEvent<User> event) {
			events.add(event);
		}

		@Override
		public void onAfterLoad(AfterLoadEvent<User> event) {
			events.add(event);
		}

		@Override
		public void onAfterConvert(AfterConvertEvent<User> event) {
			events.add(event);
		}

		private void clear() {
			events.clear();
		}

		private List<BeforeSaveEvent<User>> getBeforeSave() {
			return filter(BeforeSaveEvent.class);
		}

		private List<AfterSaveEvent<User>> getAfterSave() {
			return filter(AfterSaveEvent.class);
		}

		private List<BeforeDeleteEvent<User>> getBeforeDelete() {
			return filter(BeforeDeleteEvent.class);
		}

		private List<AfterDeleteEvent<User>> getAfterDelete() {
			return filter(AfterDeleteEvent.class);
		}

		List<AfterConvertEvent<User>> getAfterConvert() {
			return filter(AfterConvertEvent.class);
		}

		List<AfterLoadEvent<User>> getAfterLoad() {
			return filter(AfterLoadEvent.class);
		}

		@SuppressWarnings("unchecked")
		private <T> List<T> filter(Class<? super T> targetType) {
			return (List) events.stream().filter(targetType::isInstance).map(targetType::cast).collect(Collectors.toList());
		}
	}

	interface CassandraOperationsAccessor {

		void insert(Object entity);

		void update(Object entity);

		void delete(Object entity);

		void deleteById(Object id, Class<?> entityClass);

		void delete(Query query, Class<?> entityClass);

		void truncate(Class<?> entityClass);

		<T> T selectOneById(String id, Class<T> entityClass);

		<T> List<T> select(Query query, Class<T> entityClass);

		<T> List<T> select(SimpleStatement statement, Class<T> entityClass);
	}
}
