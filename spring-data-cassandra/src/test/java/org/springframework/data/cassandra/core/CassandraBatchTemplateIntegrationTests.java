/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.domain.FlatGroup;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.cassandra.domain.GroupKey;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * Integration tests for {@link CassandraBatchTemplate}.
 * 
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraBatchTemplateIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Group.class.getPackage().getName() };
		}
	}

	@Autowired CassandraTemplate cassandraTemplate;

	@Before
	public void setUp() throws Exception {
		cassandraTemplate.deleteAll(Group.class);
	}

	/**
	 * @see DATACASS-288
	 */
	@Test
	public void shouldInsertEntities() {

		Group walter = new Group(new GroupKey("users", "0x1", "walter"));
		Group mike = new Group(new GroupKey("users", "0x1", "mike"));

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);
		batchOperations.insert(walter).insert(mike).execute();

		Group loaded = cassandraTemplate.selectOneById(Group.class, walter.getId());
		assertThat(loaded.getId().getUsername(), is(equalTo(walter.getId().getUsername())));
	}

	/**
	 * @see DATACASS-288
	 */
	@Test
	public void shouldInsertCollectionOfEntities() {

		Group walter = new Group(new GroupKey("users", "0x1", "walter"));
		Group mike = new Group(new GroupKey("users", "0x1", "mike"));

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);
		batchOperations.insert(Arrays.asList(walter, mike)).execute();

		Group loaded = cassandraTemplate.selectOneById(Group.class, walter.getId());
		assertThat(loaded.getId().getUsername(), is(equalTo(walter.getId().getUsername())));
	}

	/**
	 * @see DATACASS-288
	 */
	@Test
	public void shouldUpdateEntities() {

		Group walter = cassandraTemplate.insert(new Group(new GroupKey("users", "0x1", "walter")));
		Group mike = cassandraTemplate.insert(new Group(new GroupKey("users", "0x1", "mike")));

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);
		batchOperations.update(walter).update(mike).execute();

		Group loaded = cassandraTemplate.selectOneById(Group.class, walter.getId());
		assertThat(loaded.getEmail(), is(equalTo(walter.getEmail())));
	}

	/**
	 * @see DATACASS-288
	 */
	@Test
	public void shouldUpdateCollectionOfEntities() {

		Group walter = cassandraTemplate.insert(new Group(new GroupKey("users", "0x1", "walter")));
		Group mike = cassandraTemplate.insert(new Group(new GroupKey("users", "0x1", "mike")));

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);
		batchOperations.update(Arrays.asList(walter, mike)).execute();

		Group loaded = cassandraTemplate.selectOneById(Group.class, walter.getId());
		assertThat(loaded.getEmail(), is(equalTo(walter.getEmail())));
	}

	/**
	 * @see DATACASS-288
	 */
	@Test
	public void shouldUpdatesCollectionOfEntities() {

		FlatGroup walter = cassandraTemplate.insert(new FlatGroup("users", "0x1", "walter"));
		FlatGroup mike = cassandraTemplate.insert(new FlatGroup("users", "0x1", "mike"));

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);
		batchOperations.update(Arrays.asList(walter, mike)).execute();

		FlatGroup loaded = cassandraTemplate.selectOneById(FlatGroup.class, walter);
		assertThat(loaded.getEmail(), is(equalTo(walter.getEmail())));
	}

	/**
	 * @see DATACASS-288
	 */
	@Test
	public void shouldDeleteEntities() {

		Group walter = cassandraTemplate.insert(new Group(new GroupKey("users", "0x1", "walter")));
		Group mike = cassandraTemplate.insert(new Group(new GroupKey("users", "0x1", "mike")));

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);

		batchOperations.delete(walter).delete(mike).execute();

		Group loaded = cassandraTemplate.selectOneById(Group.class, walter.getId());
		assertThat(loaded, is(nullValue()));
	}

	/**
	 * @see DATACASS-288
	 */
	@Test
	public void shouldDeleteCollectionOfEntities() {

		Group walter = cassandraTemplate.insert(new Group(new GroupKey("users", "0x1", "walter")));
		Group mike = cassandraTemplate.insert(new Group(new GroupKey("users", "0x1", "mike")));

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);

		batchOperations.delete(Arrays.asList(walter, mike)).execute();

		Group loaded = cassandraTemplate.selectOneById(Group.class, walter.getId());
		assertThat(loaded, is(nullValue()));
	}

	/**
	 * @see DATACASS-288
	 */
	@Test
	public void shouldApplyTimestampToAllEntities() {

		Group walter = new Group(new GroupKey("users", "0x1", "walter"));
		Group mike = new Group(new GroupKey("users", "0x1", "mike"));

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		long timestamp = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)) * 1000;

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);
		batchOperations.insert(walter).insert(mike).withTimestamp(timestamp).execute();

		ResultSet resultSet = cassandraTemplate.query("SELECT writetime(email) FROM group;");

		assertThat(resultSet.getAvailableWithoutFetching(), is(2));
		for (Row row : resultSet) {
			assertThat(row.getLong(0), is(timestamp));
		}
	}

	/**
	 * @see DATACASS-288
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldNotExecuteTwice() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);
		batchOperations.insert(new Group(new GroupKey("users", "0x1", "walter"))).execute();

		batchOperations.execute();
		fail("Missing IllegalStateException");
	}

	/**
	 * @see DATACASS-288
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldNotAllowModificationAfterExecution() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(cassandraTemplate);
		batchOperations.insert(new Group(new GroupKey("users", "0x1", "walter"))).execute();

		batchOperations.update(new Group());
		fail("Missing IllegalStateException");
	}
}
