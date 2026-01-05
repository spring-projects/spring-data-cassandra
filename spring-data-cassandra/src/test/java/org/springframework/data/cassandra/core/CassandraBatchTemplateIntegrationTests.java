/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.domain.FlatGroup;
import org.springframework.data.cassandra.domain.Group;
import org.springframework.data.cassandra.domain.GroupKey;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * Integration tests for {@link CassandraBatchTemplate}.
 *
 * @author Mark Paluch
 * @author Anup Sabbi
 */
class CassandraBatchTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraTemplate template;

	private Group walter = new Group(new GroupKey("users", "0x1", "walter"));
	private Group mike = new Group(new GroupKey("users", "0x1", "mike"));

	@BeforeEach
	void setUp() throws Exception {

		template = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(Group.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(FlatGroup.class, template);

		SchemaTestUtils.truncate(Group.class, template);
		SchemaTestUtils.truncate(FlatGroup.class, template);

		template.insert(walter);
		template.insert(mike);
	}

	@Test // GH-1499
	void shouldAddStatements() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);

		List<SimpleStatement> statements = List.of(SimpleStatement
				.newInstance("INSERT INTO GROUP (groupname, hash_prefix, username) VALUES('users', '0x1', 'walter')"));

		batchOperations.addStatements(statements).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername());
	}

	@Test // GH-1499
	void insertUpdateDeleteShouldRejectStatements() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);

		SimpleStatement statement = SimpleStatement.newInstance("FOO");

		assertThatIllegalArgumentException().isThrownBy(() -> batchOperations.insert(statement));
		assertThatIllegalArgumentException().isThrownBy(() -> batchOperations.update(statement));
		assertThatIllegalArgumentException().isThrownBy(() -> batchOperations.delete(statement));
	}

	@Test // DATACASS-288
	void shouldInsertEntities() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.insert(walter).insert(mike).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername());
	}

	@Test // #1135
	void insertAsVarargsShouldRejectQueryOptions() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> template.batchOps().insert(mike, walter, InsertOptions.empty()));
	}

	@Test // DATACASS-288
	void shouldInsertEntitiesWithLwt() {

		InsertOptions lwtOptions = InsertOptions.builder().withIfNotExists().build();

		Group previousWalter = new Group(new GroupKey("users", "0x1", "walter"));
		previousWalter.setAge(42);
		template.insert(previousWalter);

		walter.setAge(100);

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);

		WriteResult writeResult = batchOperations.insert(walter, lwtOptions).insert(mike).execute();

		Group loadedWalter = template.selectOneById(walter.getId(), Group.class);
		Group loadedMike = template.selectOneById(mike.getId(), Group.class);

		assertThat(loadedWalter.getId().getUsername()).isEqualTo(walter.getId().getUsername());
		assertThat(loadedWalter.getAge()).isEqualTo(42);

		assertThat(loadedMike).isNotNull();
		assertThat(writeResult.wasApplied()).isFalse();
		assertThat(writeResult.getExecutionInfo()).isNotEmpty();
		assertThat(writeResult.getRows()).isNotEmpty();
	}

	@Test // DATACASS-288
	void shouldInsertCollectionOfEntities() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.insert(Arrays.asList(walter, mike)).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getId().getUsername()).isEqualTo(walter.getId().getUsername());
	}

	@Test // DATACASS-443
	void shouldInsertCollectionOfEntitiesWithTtl() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		int ttl = 30;
		WriteOptions options = WriteOptions.builder().ttl(30).build();

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.insert(Arrays.asList(walter, mike), options).execute();

		ResultSet resultSet = template.getCqlOperations().queryForResultSet("SELECT TTL(email) FROM group;");

		assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);

		for (Row row : resultSet) {
			assertThat(row.getInt(0)).isBetween(1, ttl);
		}
	}

	@Test // #1135
	void updateAsVarargsShouldRejectQueryOptions() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> template.batchOps().update(mike, walter, InsertOptions.empty()));
	}

	@Test // DATACASS-288
	void shouldUpdateEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.update(walter).update(mike).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getEmail()).isEqualTo(walter.getEmail());
	}

	@Test // DATACASS-288
	void shouldUpdateCollectionOfEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.update(Arrays.asList(walter, mike)).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded.getEmail()).isEqualTo(walter.getEmail());
	}

	@Test // DATACASS-443
	void shouldUpdateCollectionOfEntitiesWithTtl() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		int ttl = 30;
		WriteOptions options = WriteOptions.builder().ttl(ttl).build();

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.update(walter, options).execute();

		ResultSet resultSet = template.getCqlOperations().queryForResultSet("SELECT TTL(email), email FROM group");

		assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);

		for (Row row : resultSet) {

			if (walter.getEmail().equals(row.getString("email"))) {
				assertThat(row.getInt(0)).isBetween(1, ttl);
			} else {
				assertThat(row.getInt(0)).isZero();
			}
		}
	}

	@Test // DATACASS-288
	void shouldUpdatesCollectionOfEntities() {

		FlatGroup walter = new FlatGroup("users", "0x1", "walter");
		FlatGroup mike = new FlatGroup("users", "0x1", "mike");

		template.insert(walter);
		template.insert(mike);

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.update(Arrays.asList(walter, mike)).execute();

		FlatGroup loaded = template.selectOneById(walter, FlatGroup.class);

		assertThat(loaded.getEmail()).isEqualTo(walter.getEmail());
	}

	@Test // #1135
	void deleteAsVarargsShouldRejectQueryOptions() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> template.batchOps().delete(mike, walter, InsertOptions.empty()));
	}

	@Test // DATACASS-288
	void shouldDeleteEntities() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);

		batchOperations.delete(walter).delete(mike).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded).isNull();
	}

	@Test // DATACASS-288
	void shouldDeleteCollectionOfEntities() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);

		batchOperations.delete(Arrays.asList(walter, mike)).execute();

		Group loaded = template.selectOneById(walter.getId(), Group.class);

		assertThat(loaded).isNull();
	}

	@Test // DATACASS-288
	void shouldApplyTimestampToAllEntities() {

		walter.setEmail("walter@white.com");
		mike.setEmail("mike@sauls.com");

		long timestamp = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)) * 1000;

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.insert(walter).insert(mike).withTimestamp(timestamp).execute();

		ResultSet resultSet = template.getCqlOperations().queryForResultSet("SELECT writetime(email) FROM group;");

		assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);

		for (Row row : resultSet) {
			assertThat(row.getLong(0)).isEqualTo(timestamp);
		}
	}

	@Test // GH-1192
	void shouldApplyQueryOptions() {

		QueryOptions options = QueryOptions.builder().consistencyLevel(ConsistencyLevel.THREE).build();

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		CassandraBatchOperations ops = batchOperations.insert(walter).withQueryOptions(options);

		assertThatExceptionOfType(CassandraConnectionFailureException.class).isThrownBy(ops::execute)
				.withRootCauseInstanceOf(AllNodesFailedException.class);
	}

	@Test // GH-1192
	void shouldNotExecuteTwice() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.insert(walter).execute();

		assertThatIllegalStateException().isThrownBy(() -> batchOperations.execute());
	}

	@Test // DATACASS-288
	void shouldNotAllowModificationAfterExecution() {

		CassandraBatchOperations batchOperations = new CassandraBatchTemplate(template, BatchType.LOGGED);
		batchOperations.insert(walter).execute();

		assertThatIllegalStateException().isThrownBy(() -> batchOperations.update(new Group()));
	}
}
