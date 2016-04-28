/*
 * Copyright 2013-2014 the original author or authors
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
package org.springframework.data.cassandra.test.integration.template.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.springframework.data.cassandra.repository.support.BasicMapId.id;

import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
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
 * Asynchronous {@link CassandraTemplate} tests.
 * 
 * @author Matthew T. Adams
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AsynchronousCassandraTemplateTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	public static class Config extends IntegrationTestConfig {
	}

	@Table
	public static class Thing {

		public static final String uuid() {
			return UUID.randomUUID().toString();
		}

		static final Random RNG = new Random();

		public static Thing random() {
			return new Thing(uuid(), RNG.nextInt());
		}

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		public String stuff;

		@Column
		public int number;

		public Thing() {
		}

		public Thing(String stuff, int number) {
			this.stuff = stuff;
			this.number = number;
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

	public static class ThingWriteListener extends TestListener implements WriteListener<Thing> {

		public Exception exception;
		public Collection<Thing> entities;

		@Override
		public void onWriteComplete(Collection<Thing> entities) {
			this.entities = entities;
			countDown();
		}

		@Override
		public void onException(Exception x) {
			this.exception = x;
			countDown();
		}
	}

	public static class ThingDeletionListener extends TestListener implements DeletionListener<Thing> {

		public Exception exception;
		public Collection<Thing> entities;

		@Override
		public void onDeletionComplete(Collection<Thing> entities) {
			this.entities = entities;
			countDown();
		}

		@Override
		public void onException(Exception x) {
			this.exception = x;
			countDown();
		}
	}

	@Autowired
	CassandraOperations t;

	@Before
	public void before() {
		deleteAllEntities();
	}

	public void testInsertAsynchronously(ConsistencyLevel cl) throws Exception {
		Thing thing = Thing.random();
		ThingWriteListener listener = new ThingWriteListener();
		t.insertAsynchronously(thing, listener, new WriteOptions(cl, RetryPolicy.LOGGING));
		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}
		assertEquals(thing, listener.entities.iterator().next());
	}

	@Test
	public void testInsertAsynchronously() throws Exception {
		testInsertAsynchronously(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void testInsertAsynchronouslyThrows() throws Exception {
		testInsertAsynchronously(ConsistencyLevel.TWO);
	}

	public void testInsertOrUpdateAsynchronouslyCancelled(boolean insert) throws Exception {
		Thing thing = Thing.random();
		ThingWriteListener listener = new ThingWriteListener();
		(insert ? t.insertAsynchronously(thing, listener, null) : t.updateAsynchronously(thing, listener, null)).cancel();
		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}
		fail("should've thrown CancellationException");
	}

	@Test(expected = CancellationException.class)
	public void testInsertAsynchronouslyCancelled() throws Exception {
		testInsertOrUpdateAsynchronouslyCancelled(true);
	}

	@Test(expected = CancellationException.class)
	@Ignore
	public void testUpdateAsynchronouslyCancelled() throws Exception {
		testInsertOrUpdateAsynchronouslyCancelled(false);
	}

	public void testUpdateAsynchronously(ConsistencyLevel cl) throws Exception {
		Thing thing = Thing.random();
		t.insert(thing);
		thing.number = Thing.random().number;
		ThingWriteListener listener = new ThingWriteListener();
		t.updateAsynchronously(thing, listener, new WriteOptions(cl, RetryPolicy.LOGGING));
		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}
		assertEquals(thing, listener.entities.iterator().next());
	}

	@Test
	public void testUpdateAsynchronously() throws Exception {
		testUpdateAsynchronously(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void testUpdateAsynchronouslyThrows() throws Exception {
		testUpdateAsynchronously(ConsistencyLevel.TWO);
	}

	public void testDeleteAsynchronously(ConsistencyLevel cl) throws Exception {
		Thing thing = Thing.random();
		t.insert(thing);
		ThingDeletionListener listener = new ThingDeletionListener();
		t.deleteAsynchronously(thing, listener, new WriteOptions(cl, RetryPolicy.LOGGING));
		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}
		assertFalse(t.exists(Thing.class, id("stuff", thing.stuff)));
	}

	@Test
	public void testDeleteAsynchronously() throws Exception {
		testDeleteAsynchronously(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void testDeleteAsynchronouslyThrows() throws Exception {
		testDeleteAsynchronously(ConsistencyLevel.TWO);
	}

	@Test(expected = CancellationException.class)
	public void testDeleteAsynchronouslyCancelled() throws Exception {
		Thing thing = Thing.random();
		ThingDeletionListener listener = new ThingDeletionListener();
		t.deleteAsynchronously(thing, listener, null).cancel();
		listener.await();
		if (listener.exception != null) {
			throw listener.exception;
		}
		fail("should've thrown CancellationException");
	}

}
