/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.mapping.event.AbstractCassandraEventListener;
import org.springframework.data.cassandra.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterLoadEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterSaveEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.domain.Slice;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for callback events.
 *
 * @author Lukasz Antoniak
 */
public class EventListenerIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {
	private static final CaptureEventListener listener = new CaptureEventListener();

	private CassandraTemplate template = null;
	private ConfigurableApplicationContext context = null;
	private User firstUser = null;

	@Before
	public void setUp() {
		context = new AnnotationConfigApplicationContext(ListenerConfiguration.class);
		setUpTemplate(session, context);

		firstUser = new User("id-1", "Johny", "Bravo");
		insert(firstUser);

		listener.clear();
	}

	@After
	public void tearDown() {
		tearDownTemplate();
		if (context != null) {
			context.close();
			context = null;
		}
	}

	@Test // DATACASS-106
	public void shouldEmitInsertEvents() {
		User user = new User("id-2", "Lukasz", "Antoniak");
		insert(user);

		assertThat(listener.getBeforeSave()).isEqualTo(Collections.singletonList(user));
		assertThat(listener.getAfterSave()).isEqualTo(Collections.singletonList(user));
	}

	@Test // DATACASS-106
	public void shouldEmitUpdateEvents() {
		firstUser.setLastname("Wayne");
		update(firstUser);

		assertThat(listener.getBeforeSave()).isEqualTo(Collections.singletonList(firstUser));
		assertThat(listener.getAfterSave()).isEqualTo(Collections.singletonList(firstUser));
	}

	@Test // DATACASS-106
	public void shouldEmitDeleteEvents() {
		delete(firstUser);

		assertThat(listener.getBeforeDelete()).isEqualTo(Collections.singletonList(firstUser));
		assertThat(listener.getAfterDelete()).isEqualTo(Collections.singletonList(firstUser));
	}

	@Test // DATACASS-106
	public void shouldEmitLoadEvents() {
		User user = new User("id-2", "Lukasz", "Antoniak");
		insert(user);

		User loaded = selectOneById(firstUser.getId(), User.class);
		assertThat(listener.getAfterLoad()).isEqualTo(Collections.singletonList(loaded));

		listener.clear();

		stream("SELECT * FROM users;", User.class).count(); // Just load entire stream.
		assertThat(listener.getAfterLoad()).isEqualTo(Arrays.asList(loaded, user));

		listener.clear();

		slice(Query.empty(), User.class).getSize(); // Force load entire collection.
		assertThat(listener.getAfterLoad()).isEqualTo(Arrays.asList(loaded, user));
	}

	@Test // DATACASS-106
	public void shouldEmitMultipleEvents() {
		User user = new User("id-2", "Lukasz", "Antoniak");
		insert(user);
		user.setFirstname("Robert");
		update(user);
		User loaded = selectOneById("id-1", User.class);
		delete(loaded);

		List<User> modificationHistory = Arrays.asList(new User("id-2", "Lukasz", "Antoniak"), new User("id-2", "Robert", "Antoniak"));
		assertThat(listener.getBeforeSave()).isEqualTo(modificationHistory);
		assertThat(listener.getAfterSave()).isEqualTo(modificationHistory);
		assertThat(listener.getBeforeDelete()).isEqualTo(Collections.singletonList(loaded));
		assertThat(listener.getAfterDelete()).isEqualTo(Collections.singletonList(loaded));
		assertThat(listener.getAfterLoad()).isEqualTo(Collections.singletonList(loaded));
	}

	@Configuration
	static abstract class ListenerConfiguration {
		@Bean
		public ApplicationListener listener() {
			return listener;
		}
	}

	private static class CaptureEventListener extends AbstractCassandraEventListener<User> {
		private final List<User> beforeSave = new LinkedList<User>();
		private final List<User> afterSave = new LinkedList<User>();
		private final List<User> beforeDelete = new LinkedList<User>();
		private final List<User> afterDelete = new LinkedList<User>();
		private final List<User> afterLoad = new LinkedList<User>();

		@Override
		public void onBeforeSave(BeforeSaveEvent<User> event) {
			super.onBeforeSave(event);
			beforeSave.add(event.getSource());
			assertThat(event.getTable()).isEqualTo("users");
			assertThat(event.getStatement()).isNotNull();
		}

		@Override
		public void onAfterSave(AfterSaveEvent<User> event) {
			super.onAfterSave(event);
			afterSave.add(event.getSource());
			assertThat(event.getTable()).isEqualTo("users");
		}

		@Override
		public void onBeforeDelete(BeforeDeleteEvent<User> event) {
			super.onBeforeDelete(event);
			beforeDelete.add(event.getSource());
			assertThat(event.getTable()).isEqualTo("users");
			assertThat(event.getStatement()).isNotNull();
		}

		@Override
		public void onAfterDelete(AfterDeleteEvent<User> event) {
			super.onAfterDelete(event);
			afterDelete.add(event.getSource());
			assertThat(event.getTable()).isEqualTo("users");
		}

		@Override
		public void onAfterLoad(AfterLoadEvent<User> event) {
			super.onAfterLoad(event);
			afterLoad.add(event.getSource());
			assertThat(event.getTable()).isEqualTo("users");
		}

		private void clear() {
			beforeSave.clear();
			afterSave.clear();
			beforeDelete.clear();
			afterDelete.clear();
			afterLoad.clear();
		}

		private List<User> getBeforeSave() {
			return beforeSave;
		}

		private List<User> getAfterSave() {
			return afterSave;
		}

		private List<User> getBeforeDelete() {
			return beforeDelete;
		}

		private List<User> getAfterDelete() {
			return afterDelete;
		}

		private List<User> getAfterLoad() {
			return afterLoad;
		}
	}

	protected void setUpTemplate(Session session, ConfigurableApplicationContext context) {
		template = new CassandraTemplate(session);
		template.setApplicationContext(context);

		SchemaTestUtils.potentiallyCreateTableFor(User.class, template);
		SchemaTestUtils.truncate(User.class, template);
	}

	protected void tearDownTemplate() {
		template = null;
	}

	protected void insert(Object entity) {
		template.insert(entity);
	}

	protected void update(Object entity) {
		template.update(entity);
	}

	protected void delete(Object entity) {
		template.delete(entity);
	}

	protected <T> T selectOneById(String id, Class<T> entityClass) {
		return template.selectOneById(id, entityClass);
	}

	protected <T> Slice<T> slice(Query query, Class<T> entityClass) {
		return template.slice(query, entityClass);
	}

	protected <T> Stream<T> stream(String statement, Class<T> entityClass) {
		return template.stream(statement, entityClass);
	}
}
