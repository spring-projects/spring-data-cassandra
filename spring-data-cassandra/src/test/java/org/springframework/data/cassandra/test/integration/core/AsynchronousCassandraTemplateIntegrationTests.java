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

import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.cassandra.repository.support.BasicMapId.*;

import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;

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

		Thing thing = Thing.random();
		ThingListener listener = new ThingListener();

		cassandraOperations.insertAsynchronously(thing, listener, new WriteOptions(cl, RetryPolicy.LOGGING));
		listener.await();

		if (listener.exception != null) {
			throw listener.exception;
		}

		assertEquals(thing, listener.entities.iterator().next());
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

		Thing thing = Thing.random();
		ThingListener listener = new ThingListener();

		Cancellable cancellable;

		if (insert) {
			cancellable = cassandraOperations.insertAsynchronously(thing, listener, null);
		} else {
			cancellable = cassandraOperations.updateAsynchronously(thing, listener, null);
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

		Thing thing = Thing.random();
		cassandraOperations.insert(thing);
		thing.number = Thing.random().number;

		ThingListener listener = new ThingListener();
		cassandraOperations.updateAsynchronously(thing, listener, new WriteOptions(cl, RetryPolicy.LOGGING));

		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}

		assertEquals(thing, listener.entities.iterator().next());
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

		Thing thing = Thing.random();

		cassandraOperations.insert(thing);

		ThingListener listener = new ThingListener();
		cassandraOperations.deleteAsynchronously(thing, listener, new WriteOptions(cl, RetryPolicy.LOGGING));

		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}
		assertFalse(cassandraOperations.exists(Thing.class, id("stuff", thing.stuff)));
	}

	@Test(expected = CancellationException.class)
	public void deleteAsynchronouslyCancelled() throws Exception {

		Thing thing = Thing.random();
		ThingListener listener = new ThingListener();
		cassandraOperations.deleteAsynchronously(thing, listener, null).cancel();
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

	@Configuration
	public static class Config extends IntegrationTestConfig {}

	@Table
	public static class Thing {

		private static final Random RNG = new Random();

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) public String stuff;
		@Column public int number;

		public Thing() {}

		public Thing(String stuff, int number) {
			this.stuff = stuff;
			this.number = number;
		}

		public static final String uuid() {
			return UUID.randomUUID().toString();
		}

		public static Thing random() {
			return new Thing(uuid(), RNG.nextInt());
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + number;
			result = prime * result + ((stuff == null) ? 0 : stuff.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Thing other = (Thing) obj;
			if (number != other.number)
				return false;
			if (stuff == null) {
				if (other.stuff != null)
					return false;
			} else if (!stuff.equals(other.stuff))
				return false;
			return true;
		}
	}

	public static class ThingListener extends TestListener implements WriteListener<Thing>, DeletionListener<Thing> {

		public volatile Exception exception;
		public volatile Collection<Thing> entities;
		public volatile boolean success;

		@Override
		public void onWriteComplete(Collection<Thing> entities) {
			this.entities = entities;
			this.success = true;
			countDown();
		}

		@Override
		public void onDeletionComplete(Collection<Thing> entities) {
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
