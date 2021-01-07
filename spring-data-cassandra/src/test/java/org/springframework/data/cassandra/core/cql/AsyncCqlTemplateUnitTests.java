/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;

/**
 * Unit tests for {@link AsyncCqlTemplate}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AsyncCqlTemplateUnitTests {

	@Mock CqlSession session;
	@Mock AsyncResultSet resultSet;
	@Mock Row row;
	@Mock PreparedStatement preparedStatement;
	@Mock BoundStatement boundStatement;
	@Mock ColumnDefinitions columnDefinitions;

	private AsyncCqlTemplate template;

	@BeforeEach
	void setup() {

		this.template = new AsyncCqlTemplate();
		this.template.setSession(session);
	}

	// -------------------------------------------------------------------------
	// Tests dealing with a plain com.datastax.oss.driver.api.core.CqlSession
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	void executeCallbackShouldTranslateExceptions() {

		try {
			template.execute((AsyncSessionCallback<String>) session -> {
				throw new InvalidQueryException(null, "wrong query");
			});

			fail("Missing CassandraInvalidQueryException");
		} catch (CassandraInvalidQueryException e) {
			assertThat(e).hasMessageContaining("wrong query");
		}
	}

	@Test // DATACASS-292
	void executeCqlShouldTranslateExceptions() throws Exception {

		TestResultSetFuture resultSetFuture = TestResultSetFuture.failed(new NoNodeAvailableException());
		when(session.executeAsync(any(Statement.class))).thenReturn(resultSetFuture);

		ListenableFuture<Boolean> future = template.execute("UPDATE user SET a = 'b';");

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("No node was available");
		}
	}

	// -------------------------------------------------------------------------
	// Tests dealing with static CQL
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	void executeCqlShouldCallExecution() {

		doTestStrings(asyncCqlTemplate -> {

			asyncCqlTemplate.execute("SELECT * from USERS");

			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-767
	void executePreparedStatementShouldApplyKeyspace() {

		doTestStrings(null, null, CqlIdentifier.fromCql("ks1"), cqlTemplate -> {
			cqlTemplate.execute("SELECT * from USERS", (session, ps) -> session.executeAsync(ps.bind("A")));
		});

		ArgumentCaptor<SimpleStatement> captor = ArgumentCaptor.forClass(SimpleStatement.class);
		verify(session).prepareAsync(captor.capture());

		SimpleStatement statement = captor.getValue();
		assertThat(statement.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("ks1"));
	}

	@Test // DATACASS-292
	void executeCqlWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, asyncCqlTemplate -> {

			asyncCqlTemplate.execute("SELECT * from USERS");

			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryForResultSetShouldCallExecution() {

		doTestStrings(asyncCqlTemplate -> {

			AsyncResultSet resultSet = getUninterruptibly(asyncCqlTemplate.queryForResultSet("SELECT * from USERS"));

			assertThat(resultSet.currentPage()).hasSize(3);
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryWithResultSetExtractorShouldCallExecution() {

		doTestStrings(asyncCqlTemplate -> {

			List<String> rows = getUninterruptibly(
					asyncCqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0)));

			assertThat(rows).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryWithResultSetExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, asyncCqlTemplate -> {

			List<String> rows = getUninterruptibly(
					asyncCqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0)));

			assertThat(rows).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryCqlShouldTranslateExceptions() throws Exception {

		TestResultSetFuture resultSetFuture = TestResultSetFuture.failed(new NoNodeAvailableException());
		when(session.executeAsync(any(Statement.class))).thenReturn(resultSetFuture);

		ListenableFuture<Boolean> future = template.query("UPDATE user SET a = 'b';",
				(AsyncResultSetExtractor<Boolean>) it -> new AsyncResult<>(it.wasApplied()));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("No node was available");
		}
	}

	@Test // DATACASS-292
	void queryForObjectCqlShouldBeEmpty() throws Exception {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.emptyList());

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
	void queryForObjectCqlShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForObjectCqlShouldReturnNullValue() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user", (row, rowNum) -> null);
		assertThat(getUninterruptibly(future)).isNull();
	}

	@Test // DATACASS-292
	void queryForObjectCqlShouldFailReturningManyRecords() throws Exception {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Arrays.asList(row, row));

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
	void queryForObjectCqlWithTypeShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user", String.class);

		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForListCqlWithTypeShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Arrays.asList(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		ListenableFuture<List<String>> future = template.queryForList("SELECT * FROM user", String.class);

		assertThat(getUninterruptibly(future)).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	void executeCqlShouldReturnWasApplied() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.wasApplied()).thenReturn(true);

		ListenableFuture<Boolean> future = template.execute("UPDATE user SET a = 'b';");

		assertThat(getUninterruptibly(future)).isTrue();
	}

	// -------------------------------------------------------------------------
	// Tests dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	void executeStatementShouldCallExecution() {

		doTestStrings(asyncCqlTemplate -> {

			asyncCqlTemplate.execute(SimpleStatement.newInstance("SELECT * from USERS"));

			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void executeStatementWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, asyncCqlTemplate -> {

			asyncCqlTemplate.execute(SimpleStatement.newInstance("SELECT * from USERS"));

			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryForResultStatementSetShouldCallExecution() {

		doTestStrings(asyncCqlTemplate -> {

			ListenableFuture<AsyncResultSet> future = asyncCqlTemplate
					.queryForResultSet(SimpleStatement.newInstance("SELECT * from USERS"));

			assertThat(getUninterruptibly(future).currentPage()).hasSize(3);
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryWithResultSetStatementExtractorShouldCallExecution() {

		doTestStrings(asyncCqlTemplate -> {

			ListenableFuture<List<String>> future = asyncCqlTemplate.query(SimpleStatement.newInstance("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(getUninterruptibly(future)).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryWithResultSetStatementExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, asyncCqlTemplate -> {

			ListenableFuture<List<String>> future = asyncCqlTemplate.query(SimpleStatement.newInstance("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(getUninterruptibly(future)).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).executeAsync(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryStatementShouldTranslateExceptions() throws Exception {

		TestResultSetFuture resultSetFuture = TestResultSetFuture.failed(new NoNodeAvailableException());
		when(session.executeAsync(any(Statement.class))).thenReturn(resultSetFuture);

		ListenableFuture<Boolean> future = template.query(SimpleStatement.newInstance("UPDATE user SET a = 'b';"),
				(AsyncResultSetExtractor<Boolean>) rs -> new AsyncResult<>(rs.wasApplied()));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("No node was available");
		}
	}

	@Test // DATACASS-292
	void queryForObjectStatementShouldBeEmpty() throws Exception {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.emptyList());

		ListenableFuture<String> future = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
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
	void queryForObjectStatementShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<String> future = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
				(row, rowNum) -> "OK");
		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForObjectStatementShouldReturnNullValue() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<String> future = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
				(row, rowNum) -> null);
		assertThat(getUninterruptibly(future)).isNull();
	}

	@Test // DATACASS-292
	void queryForObjectStatementShouldFailReturningManyRecords() throws Exception {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Arrays.asList(row, row));

		ListenableFuture<String> future = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
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
	void queryForObjectStatementWithTypeShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		ListenableFuture<String> future = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
				String.class);

		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForListStatementWithTypeShouldReturnRecord() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Arrays.asList(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		ListenableFuture<List<String>> future = template.queryForList(SimpleStatement.newInstance("SELECT * FROM user"),
				String.class);

		assertThat(getUninterruptibly(future)).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	void executeStatementShouldReturnWasApplied() {

		when(session.executeAsync(any(Statement.class))).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.wasApplied()).thenReturn(true);

		ListenableFuture<Boolean> future = template.execute(SimpleStatement.newInstance("UPDATE user SET a = 'b';"));

		assertThat(getUninterruptibly(future)).isTrue();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with prepared statements
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	void queryPreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(asyncCqlTemplate -> {

			ListenableFuture<CompletionStage<AsyncResultSet>> futureOfFuture = asyncCqlTemplate.execute("SELECT * from USERS",
					(session, ps) -> session.executeAsync(ps.bind("A")));

			try {
				assertThat(getUninterruptibly(futureOfFuture).toCompletableFuture().get().currentPage()).hasSize(3);
			} catch (Exception e) {
				fail(e.getMessage(), e);
			}
		});
	}

	@Test // DATACASS-292
	void executePreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(asyncCqlTemplate -> {

			when(this.preparedStatement.bind("White")).thenReturn(this.boundStatement);
			when(this.resultSet.wasApplied()).thenReturn(true);

			ListenableFuture<Boolean> applied = asyncCqlTemplate.execute("UPDATE users SET name = ?", "White");

			assertThat(getUninterruptibly(applied)).isTrue();
		});
	}

	@Test // DATACASS-292
	void executePreparedStatementCreatorShouldTranslateStatementCreationExceptions() throws Exception {

		try {
			template.execute(session -> {
				throw new NoNodeAvailableException();
			}, (session, ps) -> session.executeAsync(boundStatement));
			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("No node was available");
		}

		ListenableFuture<CompletionStage<AsyncResultSet>> future = template.execute(
				session -> AsyncResult.forExecutionException(new NoNodeAvailableException()),
				(session, ps) -> session.executeAsync(boundStatement));

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("No node was available");
		}
	}

	@Test // DATACASS-292
	void executePreparedStatementCreatorShouldTranslateStatementCallbackExceptions() throws Exception {

		ListenableFuture<?> future = template.execute(session -> new AsyncResult<>(preparedStatement), (session, ps) -> {
			throw new NoNodeAvailableException();
		});

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class)
					.hasMessageContaining("No node was available");
		}
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorShouldReturnResult() {

		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<Iterable<Row>> future = template.query(session -> new AsyncResult<>(preparedStatement),
				(AsyncResultSetExtractor<Iterable<Row>>) rs -> new AsyncResult<>(rs.currentPage()));

		assertThat(getUninterruptibly(future)).contains(row);
		verify(preparedStatement).bind();
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderShouldReturnResult() {

		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<AsyncResultSet> future = template
				.query(session -> new AsyncResult<PreparedStatement>(preparedStatement), ps -> {
					ps.bind("a", "b");
					return boundStatement;
				}, (AsyncResultSetExtractor<AsyncResultSet>) AsyncResult::new);

		assertThat(getUninterruptibly(future).currentPage()).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderShouldTranslatePrepareStatementExceptions() throws Exception {

		ListenableFuture<AsyncResultSet> future = template
				.query(session -> AsyncResult.forExecutionException(new NoNodeAvailableException()), ps -> {
					ps.bind("a", "b");
					return boundStatement;
				}, (AsyncResultSetExtractor<AsyncResultSet>) AsyncResult::new);

		try {
			future.get();

			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class);
		}
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderShouldTranslateBindExceptions() throws Exception {

		ListenableFuture<AsyncResultSet> future = template.query(session -> new AsyncResult<>(preparedStatement), ps -> {
			throw new NoNodeAvailableException();
		}, (AsyncResultSetExtractor<AsyncResultSet>) AsyncResult::new);

		try {
			future.get();
			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class);
		}
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderShouldTranslateExecutionExceptions() throws Exception {

		TestResultSetFuture resultSetFuture = TestResultSetFuture.failed(new NoNodeAvailableException());

		when(session.executeAsync(boundStatement)).thenReturn(resultSetFuture);

		ListenableFuture<AsyncResultSet> future = template.query(session -> new AsyncResult<>(preparedStatement), ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, (AsyncResultSetExtractor<AsyncResultSet>) AsyncResult::new);

		try {
			future.get();
			fail("Missing CassandraConnectionFailureException");
		} catch (ExecutionException e) {
			assertThat(e).hasCauseInstanceOf(CassandraConnectionFailureException.class);
		}
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderAndMapperShouldReturnResult() {

		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<List<Row>> future = template.query(session -> new AsyncResult<>(preparedStatement), ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, (row, rowNum) -> row);

		assertThat(getUninterruptibly(future)).hasSize(1).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-292
	void queryForObjectPreparedStatementShouldBeEmpty() throws Exception {

		when(session.prepareAsync(any(SimpleStatement.class)))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.emptyList());

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
	void queryForObjectPreparedStatementShouldReturnRecord() {

		when(session.prepareAsync(any(SimpleStatement.class)))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));

		ListenableFuture<String> future = template.queryForObject("SELECT * FROM user WHERE username = ?",
				(row, rowNum) -> "OK", "Walter");
		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForObjectPreparedStatementShouldFailReturningManyRecords() throws Exception {

		when(session.prepareAsync(any(SimpleStatement.class)))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Arrays.asList(row, row));

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
	void queryForObjectPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepareAsync(any(SimpleStatement.class)))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Collections.singleton(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		Future<String> future = template.queryForObject("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		assertThat(getUninterruptibly(future)).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForListPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepareAsync(any(SimpleStatement.class)))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.currentPage()).thenReturn(Arrays.asList(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		ListenableFuture<List<String>> future = template.queryForList("SELECT * FROM user WHERE username = ?", String.class,
				"Walter");

		assertThat(getUninterruptibly(future)).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	void updatePreparedStatementShouldReturnApplied() {

		when(session.prepareAsync(any(SimpleStatement.class)))
				.thenReturn(new TestPreparedStatementFuture(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.executeAsync(boundStatement)).thenReturn(new TestResultSetFuture(resultSet));
		when(resultSet.wasApplied()).thenReturn(true);

		ListenableFuture<Boolean> future = template.execute("UPDATE user SET username = ?", "Walter");

		assertThat(getUninterruptibly(future)).isTrue();
	}

	private void doTestStrings(Consumer<AsyncCqlTemplate> cqlTemplateConsumer) {
		doTestStrings(null, null, null, cqlTemplateConsumer);
	}

	private void doTestStrings(@Nullable Integer fetchSize, @Nullable ConsistencyLevel consistencyLevel,
			@Nullable CqlIdentifier keyspace,
			Consumer<AsyncCqlTemplate> cqlTemplateConsumer) {

		String[] results = { "Walter", "Hank", "Jesse" };

		when(this.session.executeAsync((Statement) any())).thenReturn(new TestResultSetFuture(resultSet));
		when(this.resultSet.currentPage()).thenReturn(Arrays.asList(row, row, row));
		when(this.row.getString(0)).thenReturn(results[0], results[1], results[2]);
		when(this.session.prepareAsync(anyString())).thenReturn(new TestPreparedStatementFuture(this.preparedStatement));
		when(this.session.prepareAsync(any(SimpleStatement.class)))
				.thenReturn(new TestPreparedStatementFuture(this.preparedStatement));

		AsyncCqlTemplate template = new AsyncCqlTemplate();
		template.setSession(this.session);

		if (fetchSize != null) {
			template.setFetchSize(fetchSize);
		}

		if (consistencyLevel != null) {
			template.setConsistencyLevel(consistencyLevel);
		}

		if (keyspace != null) {
			template.setKeyspace(keyspace);
		}

		cqlTemplateConsumer.accept(template);

		ArgumentCaptor<Statement> statementArgumentCaptor = ArgumentCaptor.forClass(Statement.class);
		verify(this.session).executeAsync(statementArgumentCaptor.capture());

		Statement statement = statementArgumentCaptor.getValue();

		if (fetchSize != null) {
			assertThat(statement.getPageSize()).isEqualTo(fetchSize.intValue());
		}

		if (consistencyLevel != null) {
			assertThat(statement.getConsistencyLevel()).isEqualTo(consistencyLevel);
		}
	}

	private static <T> T getUninterruptibly(Future<T> future) {

		try {
			return future.get();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static class TestResultSetFuture extends CompletableFuture<AsyncResultSet> {

		private TestResultSetFuture() {}

		private TestResultSetFuture(AsyncResultSet result) {
			complete(result);
		}

		/**
		 * Create a completed future that reports a failure given {@link Throwable}.
		 *
		 * @param throwable must not be {@literal null}.
		 * @return the completed/failed {@link TestResultSetFuture}.
		 */
		private static TestResultSetFuture failed(Throwable throwable) {

			TestResultSetFuture future = new TestResultSetFuture();
			future.completeExceptionally(throwable);
			return future;
		}
	}

	private static class TestPreparedStatementFuture extends CompletableFuture<PreparedStatement> {

		public TestPreparedStatementFuture() {}

		private TestPreparedStatementFuture(PreparedStatement ps) {
			complete(ps);
		}

	}
}
