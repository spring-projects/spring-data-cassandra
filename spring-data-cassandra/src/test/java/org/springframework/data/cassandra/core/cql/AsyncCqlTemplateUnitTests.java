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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Unit tests for {@link AsyncCqlTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class AsyncCqlTemplateUnitTests {

	@Mock Session session;
	@Mock ResultSet resultSet;
	@Mock Row row;
	@Mock PreparedStatement preparedStatement;
	@Mock BoundStatement boundStatement;
	@Mock ColumnDefinitions columnDefinitions;

	AsyncCqlTemplate template;

	@Before
	public void setup() throws Exception {

		this.template = new AsyncCqlTemplate();
		this.template.setSession(session);
	}

	// -------------------------------------------------------------------------
	// Tests dealing with a plain com.datastax.driver.core.Session
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	public void executeCallbackShouldTranslateExceptions() throws Exception {

		try {
			template.execute((AsyncSessionCallback<String>) session -> {
				throw new InvalidQueryException("wrong query");
			});

			fail("Missing CassandraInvalidQueryException");
		} catch (CassandraInvalidQueryException e) {
			assertThat(e).hasMessageContaining("wrong query");
		}
	}

	@Test // DATACASS-292
	public void executeCqlShouldTranslateExceptions() throws Exception {

		TestResultSetFuture resultSetFuture = TestResultSetFuture
				.failed(new NoHostAvailableException(Collections.emptyMap()));
		when(session.executeAsync(any(Statement.class))).thenReturn(resultSetFuture);

		ListenableFuture<Boolean> future = template.execute("UPDATE user SET a = 'b';");

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("tried for query failed");
		}
	}

	// -------------------------------------------------------------------------
	// Tests dealing with static CQL
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	public void executeCqlShouldCallExecution() {

		doTestStrings(null, null, null, asyncCqlTemplate -> {

			asyncCqlTemplate.execute("SELECT * from USERS");

			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void executeCqlWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, asyncCqlTemplate -> {

			asyncCqlTemplate.execute("SELECT * from USERS");

			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void queryForResultSetShouldCallExecution() {

		doTestStrings(null, null, null, asyncCqlTemplate -> {

			ResultSet resultSet = getUninterruptibly(asyncCqlTemplate.queryForResultSet("SELECT * from USERS"));

			assertThat(resultSet).hasSize(3);
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void queryWithResultSetExtractorShouldCallExecution() {

		doTestStrings(null, null, null, asyncCqlTemplate -> {

			List<String> rows = getUninterruptibly(
					asyncCqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0)));

			assertThat(rows).hasSize(3).contains("Walter", "Hank", " Jesse");
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void queryWithResultSetExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, asyncCqlTemplate -> {

			List<String> rows = getUninterruptibly(
					asyncCqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0)));

			assertThat(rows).hasSize(3).contains("Walter", "Hank", " Jesse");
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void queryCqlShouldTranslateExceptions() throws Exception {

		TestResultSetFuture resultSetFuture = TestResultSetFuture
				.failed(new NoHostAvailableException(Collections.emptyMap()));
		when(session.executeAsync(any(Statement.class))).thenReturn(resultSetFuture);

		ListenableFuture<Boolean> future = template.query("UPDATE user SET a = 'b';", ResultSet::wasApplied);

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("tried for query failed");
		}
	}

	@Test // DATACASS-292
	public void queryForObjectCqlShouldBeEmpty() throws Exception {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");

		try {
			future.get();

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(EmptyResultDataAccessException.class)
					.hasMessageContaining("expected 1, actual 0");
		}
	}

	@Test // DATACASS-292
	public void queryForObjectCqlShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	public void queryForObjectCqlShouldReturnNullValue() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user", (row, rowNum) -> null);
		assertThat(getUninterruptibly(future)).isNull();
	}

	@Test // DATACASS-292
	public void queryForObjectCqlShouldFailReturningManyRecords() throws Exception {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
		try {
			future.get();

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(IncorrectResultSizeDataAccessException.class)
					.hasMessageContaining("expected 1, actual 2");
		}
	}

	@Test // DATACASS-292
	public void queryForObjectCqlWithTypeShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user", String.class);

		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	public void queryForListCqlWithTypeShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		ListenableFuture<List<String>> future = template.queryForList("SELECT * FROM user", String.class);

		assertThat(getUninterruptibly(future)).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	public void executeCqlShouldReturnWasApplied() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.wasApplied()).thenReturn(true);

		ListenableFuture<Boolean> future = template.execute("UPDATE user SET a = 'b';");

		assertThat(getUninterruptibly(future)).isTrue();
	}

	// -------------------------------------------------------------------------
	// Tests dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	public void executeStatementShouldCallExecution() {

		doTestStrings(null, null, null, asyncCqlTemplate -> {

			asyncCqlTemplate.execute(new SimpleStatement("SELECT * from USERS"));

			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void executeStatementWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, asyncCqlTemplate -> {

			asyncCqlTemplate.execute(new SimpleStatement("SELECT * from USERS"));

			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void queryForResultStatementSetShouldCallExecution() {

		doTestStrings(null, null, null, asyncCqlTemplate -> {

			ListenableFuture<ResultSet> future = asyncCqlTemplate
					.queryForResultSet(new SimpleStatement("SELECT * from USERS"));

			assertThat(getUninterruptibly(future)).hasSize(3);
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void queryWithResultSetStatementExtractorShouldCallExecution() {

		doTestStrings(null, null, null, asyncCqlTemplate -> {

			ListenableFuture<List<String>> future = asyncCqlTemplate.query(new SimpleStatement("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(getUninterruptibly(future)).hasSize(3).contains("Walter", "Hank", " Jesse");
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void queryWithResultSetStatementExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, asyncCqlTemplate -> {

			ListenableFuture<List<String>> future = asyncCqlTemplate.query(new SimpleStatement("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(getUninterruptibly(future)).hasSize(3).contains("Walter", "Hank", " Jesse");
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	public void queryStatementShouldTranslateExceptions() throws Exception {

		TestResultSetFuture resultSetFuture = TestResultSetFuture
				.failed(new NoHostAvailableException(Collections.emptyMap()));
		when(session.executeAsync(any(Statement.class))).thenReturn(resultSetFuture);

		ListenableFuture<Boolean> future = template.query(new SimpleStatement("UPDATE user SET a = 'b';"),
				ResultSet::wasApplied);

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("tried for query failed");
		}
	}

	@Test // DATACASS-292
	public void queryForObjectStatementShouldBeEmpty() throws Exception {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		ListenableFuture<String> future = template.queryForObject(new SimpleStatement("SELECT * FROM user"),
				(row, rowNum) -> "OK");

		try {
			future.get();

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(EmptyResultDataAccessException.class)
					.hasMessageContaining("expected 1, actual 0");
		}
	}

	@Test // DATACASS-292
	public void queryForObjectStatementShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<String> future = template.queryForObject(new SimpleStatement("SELECT * FROM user"),
				(row, rowNum) -> "OK");
		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	public void queryForObjectStatementShouldReturnNullValue() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<String> future = template.queryForObject(new SimpleStatement("SELECT * FROM user"),
				(row, rowNum) -> null);
		assertThat(getUninterruptibly(future)).isNull();
	}

	@Test // DATACASS-292
	public void queryForObjectStatementShouldFailReturningManyRecords() throws Exception {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());

		ListenableFuture<String> future = template.queryForObject(new SimpleStatement("SELECT * FROM user"),
				(row, rowNum) -> "OK");
		try {
			future.get();

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(IncorrectResultSizeDataAccessException.class)
					.hasMessageContaining("expected 1, actual 2");
		}
	}

	@Test // DATACASS-292
	public void queryForObjectStatementWithTypeShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		ListenableFuture<String> future = template.queryForObject(new SimpleStatement("SELECT * FROM user"), String.class);

		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	public void queryForListStatementWithTypeShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		ListenableFuture<List<String>> future = template.queryForList(new SimpleStatement("SELECT * FROM user"),
				String.class);

		assertThat(getUninterruptibly(future)).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	public void executeStatementShouldReturnWasApplied() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.wasApplied()).thenReturn(true);

		ListenableFuture<Boolean> future = template.execute(new SimpleStatement("UPDATE user SET a = 'b';"));

		assertThat(getUninterruptibly(future)).isTrue();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with prepared statements
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	public void queryPreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(null, null, null, asyncCqlTemplate -> {

			ListenableFuture<ResultSetFuture> futureOfFuture = asyncCqlTemplate.execute("SELECT * from USERS",
					(session, ps) -> session.executeAsync(ps.bind("A")));

			try {
				assertThat(getUninterruptibly(futureOfFuture).get()).hasSize(3);
			} catch (Exception e) {
				fail(e.getMessage(), e);
			}
		});
	}

	@Test // DATACASS-292
	public void executePreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(null, null, null, asyncCqlTemplate -> {

			when(this.preparedStatement.bind("White")).thenReturn(this.boundStatement);
			when(this.resultSet.wasApplied()).thenReturn(true);

			ListenableFuture<Boolean> applied = asyncCqlTemplate.execute("UPDATE users SET name = ?", "White");

			assertThat(getUninterruptibly(applied)).isTrue();
		});
	}

	@Test // DATACASS-292
	public void executePreparedStatementCreatorShouldTranslateStatementCreationExceptions() throws Exception {

		try {
			template.execute(session -> {
				throw new NoHostAvailableException(Collections.emptyMap());
			}, (session, ps) -> session.executeAsync(boundStatement));
			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("tried for query");
		}

		ListenableFuture<ResultSetFuture> future = template.execute(
				session -> AsyncResult.forExecutionException(new NoHostAvailableException(Collections.emptyMap())),
				(session, ps) -> session.executeAsync(boundStatement));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("tried for query");
		}
	}

	@Test // DATACASS-292
	public void executePreparedStatementCreatorShouldTranslateStatementCallbackExceptions() throws Exception {

		ListenableFuture<ResultSetFuture> future = template.execute(session -> new AsyncResult<>(preparedStatement),
				(session, ps) -> {
					throw new NoHostAvailableException(Collections.emptyMap());
				});

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("tried for query");
		}
	}

	@Test // DATACASS-292
	public void queryPreparedStatementCreatorShouldReturnResult() {

		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<Iterator<Row>> future = template.query(session -> new AsyncResult<>(preparedStatement),
				ResultSet::iterator);

		assertThat(getUninterruptibly(future)).hasSize(1).contains(row);
		verify(preparedStatement).bind();
	}

	@Test // DATACASS-292
	public void queryPreparedStatementCreatorAndBinderShouldReturnResult() {

		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<ResultSet> future = template
				.query(session -> new AsyncResult<PreparedStatement>(preparedStatement), ps -> {
					ps.bind("a", "b");
					return boundStatement;
				}, rs -> rs);

		assertThat(getUninterruptibly(future)).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-292
	public void queryPreparedStatementCreatorAndBinderShouldTranslatePrepareStatementExceptions() throws Exception {

		ListenableFuture<ResultSet> future = template.query(
				session -> AsyncResult.forExecutionException(new NoHostAvailableException(Collections.emptyMap())), ps -> {
					ps.bind("a", "b");
					return boundStatement;
				}, rs -> rs);

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class);
		}
	}

	@Test // DATACASS-292
	public void queryPreparedStatementCreatorAndBinderShouldTranslateBindExceptions() throws Exception {

		ListenableFuture<ResultSet> future = template
				.query(session -> new AsyncResult<PreparedStatement>(preparedStatement), ps -> {
					throw new NoHostAvailableException(Collections.emptyMap());
				}, rs -> rs);

		try {
			future.get();
			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class);
		}
	}

	@Test // DATACASS-292
	public void queryPreparedStatementCreatorAndBinderShouldTranslateExecutionExceptions() throws Exception {

		TestResultSetFuture resultSetFuture = TestResultSetFuture
				.failed(new NoHostAvailableException(Collections.emptyMap()));

		when(session.executeAsync(boundStatement)).thenReturn(resultSetFuture);

		ListenableFuture<ResultSet> future = template
				.query(session -> new AsyncResult<PreparedStatement>(preparedStatement), ps -> {
					ps.bind("a", "b");
					return boundStatement;
				}, rs -> rs);

		try {
			future.get();
			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class);
		}
	}

	@Test // DATACASS-292
	public void queryPreparedStatementCreatorAndBinderAndMapperShouldReturnResult() {

		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<List<Row>> future = template.query(session -> new AsyncResult<>(preparedStatement), ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, (row, rowNum) -> row);

		assertThat(getUninterruptibly(future)).hasSize(1).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-292
	public void queryForObjectPreparedStatementShouldBeEmpty() throws Exception {

		when(session.prepareAsync("SELECT * FROM user WHERE username = ?"))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user WHERE username = ?",
				(row, rowNum) -> "OK", "Walter");

		try {
			future.get();

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(EmptyResultDataAccessException.class)
					.hasMessageContaining("expected 1, actual 0");
		}
	}

	@Test // DATACASS-292
	public void queryForObjectPreparedStatementShouldReturnRecord() {

		when(session.prepareAsync("SELECT * FROM user WHERE username = ?"))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user WHERE username = ?",
				(row, rowNum) -> "OK", "Walter");
		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	public void queryForObjectPreparedStatementShouldFailReturningManyRecords() throws Exception {

		when(session.prepareAsync("SELECT * FROM user WHERE username = ?"))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user WHERE username = ?",
				(row, rowNum) -> "OK", "Walter");
		try {
			future.get();

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(IncorrectResultSizeDataAccessException.class)
					.hasMessageContaining("expected 1, actual 2");
		}
	}

	@Test // DATACASS-292
	public void queryForObjectPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepareAsync("SELECT * FROM user WHERE username = ?"))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		Future<String> future = template.queryForObject("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	public void queryForListPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepareAsync("SELECT * FROM user WHERE username = ?"))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		ListenableFuture<List<String>> future = template.queryForList("SELECT * FROM user WHERE username = ?", String.class,
				"Walter");

		assertThat(getUninterruptibly(future)).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	public void updatePreparedStatementShouldReturnApplied() {

		when(session.prepareAsync("UPDATE user SET username = ?"))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.wasApplied()).thenReturn(true);

		ListenableFuture<Boolean> future = template.execute("UPDATE user SET username = ?", "Walter");

		assertThat(getUninterruptibly(future)).isTrue();
	}

	private <T> void doTestStrings(Integer fetchSize, ConsistencyLevel consistencyLevel,
			com.datastax.driver.core.policies.RetryPolicy retryPolicy, Consumer<AsyncCqlTemplate> cqlTemplateConsumer) {

		String[] results = { "Walter", "Hank", " Jesse" };

		when(this.session.executeAsync((Statement) any())).thenReturn(new TestResultSetFuture(resultSet));
		when(this.resultSet.iterator()).thenReturn(Arrays.asList(row, row, row).iterator());

		when(this.row.getString(0)).thenReturn(results[0], results[1], results[2]);

		SettableFuture<PreparedStatement> settableFuture = SettableFuture.create();
		settableFuture.set(this.preparedStatement);

		when(this.session.prepareAsync(anyString())).thenReturn(settableFuture);

		AsyncCqlTemplate template = new AsyncCqlTemplate();
		template.setSession(this.session);

		if (fetchSize != null) {
			template.setFetchSize(fetchSize);
		}
		if (retryPolicy != null) {
			template.setRetryPolicy(retryPolicy);
		}
		if (consistencyLevel != null) {
			template.setConsistencyLevel(consistencyLevel);
		}

		cqlTemplateConsumer.accept(template);

		ArgumentCaptor<Statement> statementArgumentCaptor = ArgumentCaptor.forClass(Statement.class);
		verify(this.session).executeAsync(statementArgumentCaptor.capture());

		Statement statement = statementArgumentCaptor.getValue();

		if (statement instanceof PreparedStatement || statement instanceof BoundStatement) {

			if (fetchSize != null) {
				verify(statement).setFetchSize(fetchSize.intValue());
			}

			if (retryPolicy != null) {
				verify(statement).setRetryPolicy(retryPolicy);
			}

			if (consistencyLevel != null) {
				verify(statement).setConsistencyLevel(consistencyLevel);
			}
		} else {

			if (fetchSize != null) {
				assertThat(statement.getFetchSize()).isEqualTo(fetchSize.intValue());
			}

			if (retryPolicy != null) {
				assertThat(statement.getRetryPolicy()).isEqualTo(retryPolicy);
			}

			if (consistencyLevel != null) {
				assertThat(statement.getConsistencyLevel()).isEqualTo(consistencyLevel);
			}
		}
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

	private static class TestPreparedStatementFuture extends AbstractFuture<PreparedStatement> {

		public TestPreparedStatementFuture() {}

		public TestPreparedStatementFuture(PreparedStatement resultSet) {
			set(resultSet);
		}

		@Override
		public boolean set(PreparedStatement value) {
			return super.set(value);
		}
	}
}
