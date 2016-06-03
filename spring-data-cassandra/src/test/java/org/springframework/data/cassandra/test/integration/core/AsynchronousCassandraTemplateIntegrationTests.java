/*
 * Copyright 2013-2016 the original author or authors
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.cassandra.repository.support.BasicMapId.*;

import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.Cancellable;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.cassandra.test.integration.support.ObjectListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.DeletionListener;
import org.springframework.data.cassandra.core.WriteListener;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.data.cassandra.test.integration.support.TestListener;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for asynchronous {@link CassandraTemplate} operations.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AsynchronousCassandraTemplateIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired CassandraOperations cassandraOperations;

	@Before
	public void before() {
		deleteAllEntities();
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

		cassandraOperations.insertAsynchronously(person, listener, new WriteOptions(cl, RetryPolicy.LOGGING));
		listener.await();

		if (listener.exception != null) {
			throw listener.exception;
		}

		assertEquals(person, listener.entities.iterator().next());
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
			cancellable = cassandraOperations.insertAsynchronously(person, listener, null);
		} else {
			cancellable = cassandraOperations.updateAsynchronously(person, listener, null);
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
		cassandraOperations.insert(person);

		PersonListener listener = new PersonListener();
		cassandraOperations.updateAsynchronously(person, listener, new WriteOptions(cl, RetryPolicy.LOGGING));

		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}

		assertEquals(person, listener.entities.iterator().next());
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

		cassandraOperations.insert(person);

		PersonListener listener = new PersonListener();
		cassandraOperations.deleteAsynchronously(person, listener, new WriteOptions(cl, RetryPolicy.LOGGING));

		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}
		assertFalse(cassandraOperations.exists(Person.class, id("id", person.id)));
	}

	@Test(expected = CancellationException.class)
	public void deleteAsynchronouslyCancelled() throws Exception {

		Person person = Person.random();
		PersonListener listener = new PersonListener();
		cassandraOperations.deleteAsynchronously(person, listener, null).cancel();
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

	/**
	 * @see DATACASS-287
	 */
	@Test(timeout = 10000)
	public void shouldSelectOneAsynchronously() throws Exception {

		Person person = Person.random();
		cassandraOperations.insert(person);

		ObjectListener<Person> objectListener = ObjectListener.create();
		String cql = String.format("SELECT * from person where id = '%s'", person.id);

		cassandraOperations.selectOneAsynchronously(cql, Person.class, objectListener);
		objectListener.await();

		assertThat(objectListener.getResult(), is(notNullValue()));
		assertThat(objectListener.getResult().id, is(equalTo(person.id)));
	}

	/**
	 * @see DATACASS-287
	 */
	@Test(timeout = 10000)
	public void shouldSelectOneAsynchronouslyIfObjectIsAbsent() throws Exception {

		ObjectListener<Person> objectListener = ObjectListener.create();
		String cql = String.format("SELECT * from person where id = '%s'", "unknown");

		cassandraOperations.selectOneAsynchronously(cql, Person.class, objectListener);
		objectListener.await();

		assertThat(objectListener.getResult(), is(nullValue()));
	}

	@Configuration
	public static class Config extends IntegrationTestConfig {}

	@Table
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class Person {

		private static final Random RNG = new Random();

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) String id;
		@Column String firstname;

		public static final String uuid() {
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
