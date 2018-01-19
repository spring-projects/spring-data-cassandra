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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.domain.User;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * Unit tests for {@link AsyncCassandraTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class AsyncCassandraTemplateUnitTests {

	@Mock Session session;
	@Mock ResultSet resultSet;
	@Mock Row row;
	@Mock ColumnDefinitions columnDefinitions;

	@Captor ArgumentCaptor<Statement> statementCaptor;

	AsyncCassandraTemplate template;

	@Before
	public void setUp() {

		template = new AsyncCassandraTemplate(session);

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
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

		ListenableFuture<List<User>> list = template.select("SELECT * FROM users", User.class);

		assertThat(getUninterruptibly(list)).hasSize(1).contains(new User("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users");
	}

	@Test // DATACASS-292
	public void selectUsingCqlShouldInvokeCallbackWithMappedResults() {

		when(resultSet.spliterator()).thenReturn(Collections.singletonList(row).spliterator());
		when(columnDefinitions.contains(anyString())).thenReturn(true);
		when(columnDefinitions.getType(anyInt())).thenReturn(DataType.ascii());

		when(columnDefinitions.getIndexOf("id")).thenReturn(0);
		when(columnDefinitions.getIndexOf("firstname")).thenReturn(1);
		when(columnDefinitions.getIndexOf("lastname")).thenReturn(2);

		when(row.getObject(0)).thenReturn("myid");
		when(row.getObject(1)).thenReturn("Walter");
		when(row.getObject(2)).thenReturn("White");

		List<User> list = new ArrayList<>();

		ListenableFuture<Void> result = template.select("SELECT * FROM users", list::add, User.class);

		assertThat(getUninterruptibly(result)).isNull();
		assertThat(list).hasSize(1).contains(new User("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users");
	}

	@Test // DATACASS-292
	public void selectShouldTranslateException() throws Exception {

		when(resultSet.iterator()).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		ListenableFuture<List<User>> list = template.select("SELECT * FROM users", User.class);

		try {
			list.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoHostAvailableException.class);
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

		ListenableFuture<User> future = template.selectOne("SELECT * FROM users WHERE id='myid';", User.class);

		assertThat(getUninterruptibly(future)).isEqualTo(new User("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
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

		ListenableFuture<User> future = template.selectOneById("myid", User.class);

		assertThat(getUninterruptibly(future)).isEqualTo(new User("myid", "Walter", "White"));
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void existsShouldReturnExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<Boolean> future = template.exists("myid", User.class);

		assertThat(getUninterruptibly(future)).isTrue();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-292
	public void existsShouldReturnNonExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		ListenableFuture<Boolean> future = template.exists("myid", User.class);

		assertThat(getUninterruptibly(future)).isFalse();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users WHERE id='myid';");
	}

	@Test // DATACASS-512
	public void existsByQueryShouldReturnExistingElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<Boolean> future = template.exists(Query.empty(), User.class);

		assertThat(getUninterruptibly(future)).isTrue();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT * FROM users LIMIT 1;");
	}

	@Test // DATACASS-292
	public void countShouldExecuteCountQueryElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		ListenableFuture<Long> future = template.count(User.class);

		assertThat(getUninterruptibly(future)).isEqualTo(42L);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT count(*) FROM users;");
	}

	@Test // DATACASS-292
	public void countByQueryShouldExecuteCountQueryElement() {

		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getLong(0)).thenReturn(42L);
		when(columnDefinitions.size()).thenReturn(1);

		ListenableFuture<Long> future = template.count(Query.empty(), User.class);

		assertThat(getUninterruptibly(future)).isEqualTo(42L);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("SELECT COUNT(1) FROM users;");
	}

	@Test // DATACASS-292
	public void insertShouldInsertEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<User> future = template.insert(user);

		assertThat(getUninterruptibly(future)).isEqualTo(user);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("INSERT INTO users (firstname,id,lastname) VALUES ('Walter','heisenberg','White');");
	}

	@Test // DATACASS-292
	public void insertShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoHostAvailableException(Collections.emptyMap())));

		ListenableFuture<User> future = template.insert(new User("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void updateShouldUpdateEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<User> future = template.update(user);

		assertThat(getUninterruptibly(future)).isEqualTo(user);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString())
				.isEqualTo("UPDATE users SET firstname='Walter',lastname='White' WHERE id='heisenberg';");
	}

	@Test // DATACASS-292
	public void updateShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoHostAvailableException(Collections.emptyMap())));

		ListenableFuture<User> future = template.update(new User("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void deleteByIdShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<Boolean> future = template.deleteById(user.getId(), User.class);

		assertThat(getUninterruptibly(future)).isTrue();
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM users WHERE id='heisenberg';");
	}

	@Test // DATACASS-292
	public void deleteShouldRemoveEntity() {

		when(resultSet.wasApplied()).thenReturn(true);

		User user = new User("heisenberg", "Walter", "White");

		ListenableFuture<User> future = template.delete(user);

		assertThat(getUninterruptibly(future)).isEqualTo(user);
		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("DELETE FROM users WHERE id='heisenberg';");
	}

	@Test // DATACASS-292
	public void deleteShouldTranslateException() throws Exception {

		reset(session);
		when(session.executeAsync(any(Statement.class)))
				.thenReturn(TestResultSetFuture.failed(new NoHostAvailableException(Collections.emptyMap())));

		ListenableFuture<User> future = template.delete(new User("heisenberg", "Walter", "White"));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasRootCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	@Test // DATACASS-292
	public void truncateShouldRemoveEntities() {

		template.truncate(User.class);

		verify(session).executeAsync(statementCaptor.capture());
		assertThat(statementCaptor.getValue().toString()).isEqualTo("TRUNCATE users;");
	}

	private static <T> T getUninterruptibly(Future<T> future) {

		try {
			return future.get();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static class TestResultSetFuture extends AbstractFuture<ResultSet> implements ResultSetFuture {

		public TestResultSetFuture() {}

		public TestResultSetFuture(ResultSet resultSet) {
			set(resultSet);
		}

		@Override
		public boolean set(ResultSet value) {
			return super.set(value);
		}

		@Override
		public ResultSet getUninterruptibly() {
			return null;
		}

		@Override
		public ResultSet getUninterruptibly(long l, TimeUnit timeUnit) throws TimeoutException {
			return null;
		}

		@Override
		protected boolean setException(Throwable throwable) {
			return super.setException(throwable);
		}

		/**
		 * Create a completed future that reports a failure given {@link Throwable}.
		 *
		 * @param throwable must not be {@literal null}.
		 * @return the completed/failed {@link TestResultSetFuture}.
		 */
		public static TestResultSetFuture failed(Throwable throwable) {

			TestResultSetFuture future = new TestResultSetFuture();
			future.setException(throwable);
			return future;
		}
	}
}
