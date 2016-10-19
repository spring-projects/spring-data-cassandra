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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.domain.FlatGroup;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.cassandra.domain.GroupKey;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * Integration tests for {@link CassandraBatchTemplate}.
 *
 * @author Mark Paluch
 */
public class CassandraBatchTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraTemplate template;

	@Before
	public void setUp() throws Exception {

		template = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(Group.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(FlatGroup.class, template);

		SchemaTestUtils.truncate(Group.class, template);
		SchemaTestUtils.truncate(FlatGroup.class, template);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test
	public void shouldInsertEntities() {

		Group walter = new Group(new GroupKey("users", "0x1", "walter"));
		Group mike = new Group(new GroupKey("users", "0x1", "mike"));

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);
		batchOperations.insert(walter).insert(mike).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test
	public void shouldInsertCollectionOfEntities() {

		Group walter = new Group(new GroupKey("users", "0x1", "walter"));
		Group mike = new Group(new GroupKey("users", "0x1", "mike"));

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);
		batchOperations.insert(Arrays.asList(walter, mike)).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test
	public void shouldUpdateEntities() {

		Group walter = template.insert(new Group(new GroupKey("users", "0x1", "walter")));
		Group mike = template.insert(new Group(new GroupKey("users", "0x1", "mike")));

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);
		batchOperations.update(walter).update(mike).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getEmail()).isEqualTo(walter.getEmail());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test
	public void shouldUpdateCollectionOfEntities() {

		Group walter = template.insert(new Group(new GroupKey("users", "0x1", "walter")));
		Group mike = template.insert(new Group(new GroupKey("users", "0x1", "mike")));

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);
		batchOperations.update(Arrays.asList(walter, mike)).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getEmail()).isEqualTo(walter.getEmail());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test
	public void shouldUpdatesCollectionOfEntities() {

		FlatGroup walter = template.insert(new FlatGroup("users", "0x1", "walter"));
		FlatGroup mike = template.insert(new FlatGroup("users", "0x1", "mike"));

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);
		batchOperations.update(Arrays.asList(walter, mike)).execute();

		FlatGroup loaded = template.selectOneById(walter, FlatGroup.class);

		assertThat(loaded.getEmail()).isEqualTo(walter.getEmail());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test
	public void shouldDeleteEntities() {

		Group walter = template.insert(new Group(new GroupKey("users", "0x1", "walter")));
		Group mike = template.insert(new Group(new GroupKey("users", "0x1", "mike")));

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);

		batchOperations.delete(walter).delete(mike).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded).isNull();
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test
	public void shouldDeleteCollectionOfEntities() {

		Group walter = template.insert(new Group(new GroupKey("users", "0x1", "walter")));
		Group mike = template.insert(new Group(new GroupKey("users", "0x1", "mike")));

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);

		batchOperations.delete(Arrays.asList(walter, mike)).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded).isNull();
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test
	public void shouldApplyTimestampToAllEntities() {

		Group walter = new Group(new GroupKey("users", "0x1", "walter"));
		Group mike = new Group(new GroupKey("users", "0x1", "mike"));

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		long timestamp = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)) * 1000;

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);
		batchOperations.insert(walter).insert(mike).withTimestamp(timestamp).execute();

		ResultSet resultSet = template.getCqlOperations().queryForResultSet("SELECT writetime(email) FROM group;");

		assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);

		for (Row row : resultSet) {
			assertThat(row.getLong(0)).isEqualTo(timestamp);
		}
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldNotExecuteTwice() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);
		batchOperations.insert(new Group(new GroupKey("users", "0x1", "walter"))).execute();

		batchOperations.execute();

		fail("Missing IllegalStateException");
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-288">DATACASS-288</a>
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldNotAllowModificationAfterExecution() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template);
		batchOperations.insert(new Group(new GroupKey("users", "0x1", "walter"))).execute();

		batchOperations.update(new Group());

		fail("Missing IllegalStateException");
	}
}
