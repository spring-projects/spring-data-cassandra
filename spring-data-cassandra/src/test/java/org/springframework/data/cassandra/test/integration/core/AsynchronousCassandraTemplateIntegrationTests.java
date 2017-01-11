/*
 * Copyright 2013-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.core;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;
import static org.springframework.data.cassandra.repository.support.BasicMapId.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.Cancellable;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.integration.support.ObjectListener;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.DeletionListener;
import org.springframework.data.cassandra.core.WriteListener;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.integration.support.TestListener;

/**
 * Integration tests for asynchronous {@link CassandraTemplate} operations.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class AsynchronousCassandraTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraOperations operations;

	@Before
	public void before() {

		operations = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(Person.class, operations);
		SchemaTestUtils.truncate(Person.class, operations);
	}

	@Test
	public void insertAsynchronously() throws Exception {
		insertAsynchronously(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void insertAsynchronouslyThrows() throws Exception {
		insertAsynchronously(ConsistencyLevel.TWO);
	}

	public void insertAsynchronously(ConsistencyLevel cl) throws Exception {

		Person person = Person.random();
		PersonListener listener = new PersonListener();

		operations.insertAsynchronously(person, listener, new WriteOptions(cl, RetryPolicy.LOGGING));
		listener.await();

		if (listener.exception != null) {
			throw listener.exception;
		}

		assertThat(listener.entities.iterator().next()).isEqualTo(person);
	}

	@Test(expected = CancellationException.class)
	public void insertAsynchronouslyCancelled() throws Exception {
		insertOrUpdateAsynchronouslyCancelled(true);
	}

	@Test(expected = CancellationException.class)
	public void updateAsynchronouslyCancelled() throws Exception {
		insertOrUpdateAsynchronouslyCancelled(false);
	}

	public void insertOrUpdateAsynchronouslyCancelled(boolean insert) throws Exception {

		Person person = Person.random();
		PersonListener listener = new PersonListener();

		Cancellable cancellable;

		if (insert) {
			cancellable = operations.insertAsynchronously(person, listener, null);
		} else {
			cancellable = operations.updateAsynchronously(person, listener, null);
		}
		cancellable.cancel();
		listener.await();

		// if listener.success is true then the
		// async operations was faster than it could be cancelled so we cannot
		// verify that a CancellationException was thrown.
		assumeFalse(listener.success);

		if (listener.exception != null) {
			throw listener.exception;
		}

		fail("should've thrown CancellationException");
	}

	@Test
	public void updateAsynchronously() throws Exception {
		updateAsynchronously(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void updateAsynchronouslyThrows() throws Exception {
		updateAsynchronously(ConsistencyLevel.TWO);
	}

	public void updateAsynchronously(ConsistencyLevel cl) throws Exception {

		Person person = Person.random();
		person.setFirstname("Homer");
		operations.insert(person);

		PersonListener listener = new PersonListener();
		operations.updateAsynchronously(person, listener, new WriteOptions(cl, RetryPolicy.LOGGING));

		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}

		assertThat(listener.entities.iterator().next()).isEqualTo(person);
	}

	@Test
	public void deleteAsynchronously() throws Exception {
		deleteAsynchronously(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void deleteAsynchronouslyThrows() throws Exception {
		deleteAsynchronously(ConsistencyLevel.TWO);
	}

	public void deleteAsynchronously(ConsistencyLevel cl) throws Exception {

		Person person = Person.random();

		operations.insert(person);

		PersonListener listener = new PersonListener();
		operations.deleteAsynchronously(person, listener, new WriteOptions(cl, RetryPolicy.LOGGING));

		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}
		assertThat(operations.exists(Person.class, id("id", person.id))).isFalse();
	}

	@Test(expected = CancellationException.class)
	public void deleteAsynchronouslyCancelled() throws Exception {

		Person person = Person.random();
		PersonListener listener = new PersonListener();
		operations.deleteAsynchronously(person, listener, null).cancel();
		listener.await();

		// if listener.success is true then the
		// async operations was faster than it could be cancelled so we cannot
		// verify that a CancellationException was thrown.
		assumeFalse(listener.success);

		if (listener.exception != null) {
			throw listener.exception;
		}

		fail("should've thrown CancellationException");
	}

	@Test(timeout = 10000) // DATACASS-287
	public void shouldSelectOneAsynchronously() throws Exception {

		Person person = Person.random();
		operations.insert(person);

		ObjectListener<Person> objectListener = ObjectListener.create();
		String cql = String.format("SELECT * from person where id = '%s'", person.id);

		operations.selectOneAsynchronously(cql, Person.class, objectListener);
		objectListener.await();

		assertThat(objectListener.getResult()).isNotNull();
		assertThat(objectListener.getResult().id).isEqualTo(person.id);
	}

	@Test(timeout = 10000) // DATACASS-287
	public void shouldSelectOneAsynchronouslyIfObjectIsAbsent() throws Exception {

		ObjectListener<Person> objectListener = ObjectListener.create();
		String cql = String.format("SELECT * from person where id = '%s'", "unknown");

		operations.selectOneAsynchronously(cql, Person.class, objectListener);
		objectListener.await();

		assertThat(objectListener.getResult()).isNull();
	}

	@Table
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	@SuppressWarnings("unused")
	static class Person {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String id;
		@Column String firstname;

		public static String uuid() {
			return UUID.randomUUID().toString();
		}

		public static Person random() {
			return new Person(uuid(), null);
		}

	}

	public static class PersonListener extends TestListener implements WriteListener<Person>, DeletionListener<Person> {

		public volatile Exception exception;
		public volatile Collection<Person> entities;
		public volatile boolean success;

		@Override
		public void onWriteComplete(Collection<Person> entities) {

			this.entities = entities;
			this.success = true;
			countDown();
		}

		@Override
		public void onDeletionComplete(Collection<Person> entities) {

			this.entities = entities;
			this.success = true;
			countDown();
		}

		@Override
		public void onException(Exception x) {

			this.exception = x;
			this.success = false;
			countDown();
		}
	}
}
