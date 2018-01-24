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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.User;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.querybuilder.Batch;

/**
 * Unit tests for {@link CassandraTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraTemplateUnitTests {

	@Mock Session session;
	@Mock ResultSet resultSet;
	@Mock Row row;
	@Mock ColumnDefinitions columnDefinitions;

	@Captor ArgumentCaptor<Statement> statementCaptor;

	CassandraTemplate template;

	@Before
	public void setUp() {

		template = new CassandraTemplate(session);

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
	}

	@Test // DATACASS-292
	public void selectUsingCqlShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		List<User> list = template.select("SELECT * FROM users", User.class);

		assertThat(list).hasSize(1).contains(new User("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users");
	}

	@Test // DATACASS-292
	public void selectShouldTranslateException() throws Exception {

		when(resultSet.iterator()).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.select("SELECT * FROM users", User.class);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void selectOneShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		User user = template.selectOne("SELECT * FROM users WHERE id='myid';", User.class);

		assertThat(user).isEqualTo(new User("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void selectOneByIdShouldReturnMappedResults() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		User user = template.selectOneById("myid", User.class);

		assertThat(user).isEqualTo(new User("myid", "Walter", "White"));
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void existsShouldReturnExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		boolean exists = template.exists("myid", User.class);

		assertThat(exists).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void existsShouldReturnNonExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		boolean exists = template.exists("myid", User.class);

		assertThat(exists).isFalse();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-512
	public void existsByQueryShouldReturnExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		boolean exists = template.exists(Query.empty(), User.class);

		assertThat(exists).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users LIMIT 1;");
	}

	@Test // DATACASS-292
	public void countShouldExecuteCountQueryElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		long count = template.count(User.class);

		assertThat(count).isEqualTo(42L);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT count(*) FROM users;");
	}

	@Test // DATACASS-512
	public void countByQueryShouldExecuteCountQueryElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		long count = template.count(Query.empty(), User.class);

		assertThat(count).isEqualTo(42L);
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT COUNT(1) FROM users;");
	}

	@Test // DATACASS-292
	public void insertShouldInsertEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user);

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES ('Walter','heisenberg','White');");
	}

	@Test // DATACASS-250
	public void insertShouldInsertWithOptionsEntity() {

		InsertOptions insertOptions = InsertOptions.builder().withIfNotExists().build();

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		template.insert(user, insertOptions);

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES ('Walter','heisenberg','White') IF NOT EXISTS;");
	}

	@Test // DATACASS-292
	public void insertShouldTranslateException() throws Exception {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.insert(new User("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void insertShouldNotApplyInsert() {

		when(resultSet.wasApplied()).thenReturn(false);

		User user = new User("heisenberg", "Walter", "White");

		WriteResult writeResult = template.insert(user, InsertOptions.builder().build());

		assertThat(writeResult.wasApplied()).isFalse();
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		template.update(user);

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE users SET firstname='Walter',lastname='White' WHERE id='heisenberg';");
	}

	@Test // DATACASS-250
	public void updateShouldUpdateEntityWithOptions() {

		when(resultSet.wasApplied()).thenReturn(true);

		UpdateOptions updateOptions = UpdateOptions.builder().withIfExists().build();

		User user = new User("heisenberg", "Walter", "White");

		WriteResult writeResult = template.update(user, updateOptions);

		assertThat(writeResult.wasApplied()).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE users SET firstname='Walter',lastname='White' WHERE id='heisenberg' IF EXISTS;");
	}

	@Test // DATACASS-292
	public void updateShouldTranslateException() throws Exception {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.update(new User("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void deleteByIdShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		boolean deleted = template.deleteById(user.getId(), User.class);

		assertThat(deleted).isTrue();
		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM users WHERE id='heisenberg';");
	}

	@Test // DATACASS-292
	public void deleteShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		template.delete(user);

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM users WHERE id='heisenberg';");
	}

	@Test // DATACASS-292
	public void deleteShouldTranslateException() throws Exception {

		reset(session);
		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.delete(new User("heisenberg", "Walter", "White"));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void truncateShouldRemoveEntities() {

		template.truncate(User.class);

		verify(session).execute(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("TRUNCATE users;");
	}

	@Test // DATACASS-292
	@Ignore
	public void batchOperationsShouldCallSession() {

		template.batchOps().insert(new User()).execute();

		verify(session).execute(Mockito.any(Batch.class));
	}
}
