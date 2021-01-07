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
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;

/**
 * Unit tests for {@link CqlTemplate}.
 *
 * @author Mark Paluch
 * @author Tomasz Lelek
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CqlTemplateUnitTests {

	@Mock CqlSession session;
	@Mock ResultSet resultSet;
	@Mock Row row;
	@Mock PreparedStatement preparedStatement;
	@Mock BoundStatement boundStatement;
	@Mock ColumnDefinitions columnDefinitions;

	private CqlTemplate template;

	@BeforeEach
	void setup() {

		this.template = new CqlTemplate();
		this.template.setSession(session);
	}

	// -------------------------------------------------------------------------
	// Tests dealing with a plain com.datastax.oss.driver.api.core.CqlSession
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	void executeCallbackShouldTranslateExceptions() {

		try {
			template.execute((SessionCallback<String>) session -> {
				throw new InvalidQueryException(null, "wrong query");
			});

			fail("Missing CassandraInvalidQueryException");
		} catch (CassandraInvalidQueryException e) {
			assertThat(e).hasMessageContaining("wrong query");
		}
	}

	@Test // DATACASS-292
	void executeCqlShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		try {
			template.execute("UPDATE user SET a = 'b';");
			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("No node was available");
		}
	}

	// -------------------------------------------------------------------------
	// Tests dealing with static CQL
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	void executeCqlShouldCallExecution() {

		doTestStrings(cqlTemplate -> {

			cqlTemplate.execute("SELECT * from USERS");

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void executeCqlWithArgumentsShouldCallExecution() {

		doTestStrings(5, DefaultConsistencyLevel.ONE, null, "foo", cqlTemplate -> {

			cqlTemplate.execute("SELECT * from USERS");

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryForResultSetShouldCallExecution() {

		doTestStrings(cqlTemplate -> {

			ResultSet resultSet = cqlTemplate.queryForResultSet("SELECT * from USERS");

			assertThat(resultSet).hasSize(3);
			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryWithResultSetExtractorShouldCallExecution() {

		doTestStrings(cqlTemplate -> {

			List<String> rows = cqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0));

			assertThat(rows).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryWithResultSetExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, ConsistencyLevel.EACH_QUORUM, "foo", cqlTemplate -> {

			List<String> rows = cqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0));

			assertThat(rows).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryCqlShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		try {
			template.query("UPDATE user SET a = 'b';", ResultSet::wasApplied);
			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("No node was available");
		}
	}

	@Test // DATACASS-292
	void queryForObjectCqlShouldBeEmpty() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		try {
			template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (EmptyResultDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 0");
		}
	}

	@Test // DATACASS-292
	void queryForObjectCqlShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
		assertThat(result).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForObjectCqlShouldReturnNullValue() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject("SELECT * FROM user", (row, rowNum) -> null);
		assertThat(result).isNull();
	}

	@Test // DATACASS-292
	void queryForObjectCqlShouldFailReturningManyRecords() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());

		try {
			template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (IncorrectResultSizeDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 2");
		}
	}

	@Test // DATACASS-292
	void queryForObjectCqlWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		String result = template.queryForObject("SELECT * FROM user", String.class);

		assertThat(result).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForListCqlWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		List<String> result = template.queryForList("SELECT * FROM user", String.class);

		assertThat(result).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	void executeCqlShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.wasApplied()).thenReturn(true);

		boolean applied = template.execute("UPDATE user SET a = 'b';");

		assertThat(applied).isTrue();
	}

	// -------------------------------------------------------------------------
	// Tests dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	void executeStatementShouldCallExecution() {

		doTestStrings(cqlTemplate -> {

			cqlTemplate.execute(SimpleStatement.newInstance("SELECT * from USERS"));

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void executeStatementWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, "foo", cqlTemplate -> {

			cqlTemplate.execute(SimpleStatement.newInstance("SELECT * from USERS"));

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryForResultStatementSetShouldCallExecution() {

		doTestStrings(cqlTemplate -> {

			ResultSet resultSet = cqlTemplate.queryForResultSet(SimpleStatement.newInstance("SELECT * from USERS"));

			assertThat(resultSet).hasSize(3);
			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryWithResultSetStatementExtractorShouldCallExecution() {

		doTestStrings(cqlTemplate -> {

			List<String> result = cqlTemplate.query(SimpleStatement.newInstance("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(result).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryWithResultSetStatementExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, "foo", cqlTemplate -> {

			List<String> result = cqlTemplate.query(SimpleStatement.newInstance("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(result).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-809
	public void queryForStreamWithResultSetStatementExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, "foo", cqlTemplate -> {

			Stream<String> result = cqlTemplate.queryForStream(SimpleStatement.newInstance("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(result.collect(Collectors.toList())).hasSize(3).contains("Walter", "Hank", "Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-292
	void queryStatementShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		try {
			template.query(SimpleStatement.newInstance("UPDATE user SET a = 'b';"), ResultSet::wasApplied);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("No node was available");
		}
	}

	@Test // DATACASS-292
	void queryForObjectStatementShouldBeEmpty() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		try {
			template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"), (row, rowNum) -> "OK");

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (IncorrectResultSizeDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 0");
		}
	}

	@Test // DATACASS-292
	void queryForObjectStatementShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"), (row, rowNum) -> "OK");
		assertThat(result).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForObjectStatementShouldReturnNullValue() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"), (row, rowNum) -> null);
		assertThat(result).isNull();
	}

	@Test // DATACASS-292
	void queryForObjectStatementShouldFailReturningManyRecords() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());

		try {
			template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"), (row, rowNum) -> "OK");

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (IncorrectResultSizeDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 2");
		}
	}

	@Test // DATACASS-292
	void queryForObjectStatementWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		String result = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"), String.class);

		assertThat(result).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForListStatementWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		List<String> result = template.queryForList(SimpleStatement.newInstance("SELECT * FROM user"), String.class);

		assertThat(result).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	void executeStatementShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.wasApplied()).thenReturn(true);

		boolean applied = template.execute(SimpleStatement.newInstance("UPDATE user SET a = 'b';"));

		assertThat(applied).isTrue();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with prepared statements
	// -------------------------------------------------------------------------

	@Test // DATACASS-292
	void queryPreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(cqlTemplate -> {

			ResultSet resultSet = cqlTemplate.execute("SELECT * from USERS", (session, ps) -> session.execute(ps.bind("A")));

			try {
				assertThat(resultSet).hasSize(3);
			} catch (Exception e) {
				fail(e.getMessage(), e);
			}
		});
	}

	@Test // DATACASS-767
	void executePreparedStatementShouldApplyKeyspace() {

		when(preparedStatement.bind("A")).thenReturn(boundStatement);
		when(boundStatement.getKeyspace()).thenReturn(CqlIdentifier.fromCql("ks1"));

		doTestStrings(null, null, null, null, CqlIdentifier.fromCql("ks1"), cqlTemplate -> {
			cqlTemplate.execute("SELECT * from USERS", (session, ps) -> session.execute(ps.bind("A")));
		});

		ArgumentCaptor<SimpleStatement> captor = ArgumentCaptor.forClass(SimpleStatement.class);
		verify(session).prepare(captor.capture());

		SimpleStatement statement = captor.getValue();
		assertThat(statement.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("ks1"));
	}

	@Test // DATACASS-292
	void executePreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(cqlTemplate -> {

			when(this.preparedStatement.bind("White")).thenReturn(this.boundStatement);
			when(this.resultSet.wasApplied()).thenReturn(true);

			boolean applied = cqlTemplate.execute("UPDATE users SET name = ?", "White");

			assertThat(applied).isTrue();
		});
	}

	@Test // DATACASS-292
	void executePreparedStatementCreatorShouldTranslateStatementCreationExceptions() {

		try {
			template.execute(session -> {
				throw new NoNodeAvailableException();
			}, (session, ps) -> session.execute(boundStatement));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("No node was available");
		}
	}

	@Test // DATACASS-292
	void executePreparedStatementCreatorShouldTranslateStatementCallbackExceptions() {

		try {
			template.execute(session -> preparedStatement, (session, ps) -> {
				throw new NoNodeAvailableException();
			});

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("No node was available");
		}
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorShouldReturnResult() {

		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		Iterator<Row> iterator = template.query(session -> preparedStatement, ResultSet::iterator);

		assertThat(iterator).hasNext();
		verify(preparedStatement).bind();
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderShouldReturnResult() {

		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenAnswer(it -> Collections.singleton(row).iterator());
		when(resultSet.spliterator()).thenAnswer(it -> Collections.singleton(row).spliterator());

		ResultSet resultSet = template.query(session -> preparedStatement, ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, rs -> rs);

		assertThat(resultSet).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderShouldTranslatePrepareStatementExceptions() {

		try {
			template.query(session -> {
				throw new NoNodeAvailableException();
			}, ps -> {
				ps.bind("a", "b");
				return boundStatement;
			}, rs -> rs);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderShouldTranslateBindExceptions() {

		try {
			template.query(session -> preparedStatement, ps -> {
				throw new NoNodeAvailableException();
			}, rs -> rs);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderShouldTranslateExecutionExceptions() {

		when(session.execute(boundStatement)).thenThrow(new NoNodeAvailableException());

		try {
			template.query(session -> preparedStatement, ps -> {
				ps.bind("a", "b");
				return boundStatement;
			}, rs -> rs);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasCauseInstanceOf(NoNodeAvailableException.class);
		}
	}

	@Test // DATACASS-292
	void queryPreparedStatementCreatorAndBinderAndMapperShouldReturnResult() {

		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		List<Row> rows = template.query(session -> preparedStatement, ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, (row, rowNum) -> row);

		assertThat(rows).hasSize(1).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-809
	public void queryForStreanPreparedStatementCreatorAndBinderAndMapperShouldReturnResult() {

		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.spliterator()).thenReturn(Collections.singleton(row).spliterator());

		Stream<Row> rows = template.queryForStream(session -> preparedStatement, ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, (row, rowNum) -> row);

		assertThat(rows).hasSize(1).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-292
	void queryForObjectPreparedStatementShouldBeEmpty() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		try {
			template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK", "Walter");

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (IncorrectResultSizeDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 0");
		}
	}

	@Test // DATACASS-292
	void queryForObjectPreparedStatementShouldReturnRecord() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK", "Walter");
		assertThat(result).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForObjectPreparedStatementShouldFailReturningManyRecords() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());

		try {
			template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK", "Walter");

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (IncorrectResultSizeDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 2");
		}
	}

	@Test // DATACASS-292
	void queryForObjectPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		String result = template.queryForObject("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		assertThat(result).isEqualTo("OK");
	}

	@Test // DATACASS-292
	void queryForListPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		List<String> result = template.queryForList("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		assertThat(result).contains("OK", "NOT OK");
	}

	@Test // DATACASS-292
	void updatePreparedStatementShouldReturnApplied() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.wasApplied()).thenReturn(true);

		boolean applied = template.execute("UPDATE user SET username = ?", "Walter");

		assertThat(applied).isTrue();
	}

	@Test // DATACASS-767
	void executeCqlWithKeyspaceShouldCallExecution() {

		doTestStrings(5, DefaultConsistencyLevel.ONE, null, "foo", CqlIdentifier.fromCql("some_keyspace"), cqlTemplate -> {

			cqlTemplate.execute("SELECT * from USERS");

			verify(session).execute(any(Statement.class));
		});
	}

	private void doTestStrings(Consumer<CqlTemplate> cqlTemplateConsumer) {
		doTestStrings(null, null, null, null, null, cqlTemplateConsumer);
	}

	private void doTestStrings(@Nullable Integer fetchSize, @Nullable ConsistencyLevel consistencyLevel,
			@Nullable ConsistencyLevel serialConsistencyLevel, @Nullable String executionProfile,
			Consumer<CqlTemplate> cqlTemplateConsumer) {
		doTestStrings(fetchSize, consistencyLevel, serialConsistencyLevel, executionProfile, null, cqlTemplateConsumer);
	}

	private void doTestStrings(@Nullable Integer fetchSize, @Nullable ConsistencyLevel consistencyLevel,
			@Nullable ConsistencyLevel serialConsistencyLevel, @Nullable String executionProfile,
			@Nullable CqlIdentifier keyspace, Consumer<CqlTemplate> cqlTemplateConsumer) {

		String[] results = { "Walter", "Hank", "Jesse" };

		List<Row> rows = Arrays.asList(row, row, row);
		when(this.session.execute((Statement) any())).thenReturn(resultSet);
		when(this.resultSet.iterator()).thenReturn(rows.iterator());
		when(this.resultSet.spliterator()).thenReturn(rows.spliterator());

		when(this.row.getString(0)).thenReturn(results[0], results[1], results[2]);
		when(this.session.prepare(anyString())).thenReturn(preparedStatement);
		when(this.session.prepare(any(SimpleStatement.class))).thenReturn(preparedStatement);

		CqlTemplate template = new CqlTemplate();
		template.setSession(this.session);

		if (fetchSize != null) {
			template.setFetchSize(fetchSize);
		}

		if (consistencyLevel != null) {
			template.setConsistencyLevel(consistencyLevel);
		}

		if (serialConsistencyLevel != null) {
			template.setSerialConsistencyLevel(serialConsistencyLevel);
		}

		if (executionProfile != null) {
			template.setExecutionProfile(executionProfile);
		}

		if (keyspace != null) {
			template.setKeyspace(keyspace);
		}

		cqlTemplateConsumer.accept(template);

		ArgumentCaptor<Statement> statementArgumentCaptor = ArgumentCaptor.forClass(Statement.class);
		verify(this.session).execute(statementArgumentCaptor.capture());

		Statement statement = statementArgumentCaptor.getValue();

		if (fetchSize != null) {
			assertThat(statement.getPageSize()).isEqualTo(fetchSize.intValue());
		}

		if (consistencyLevel != null) {
			assertThat(statement.getConsistencyLevel()).isEqualTo(consistencyLevel);
		}

		if (serialConsistencyLevel != null) {
			assertThat(statement.getSerialConsistencyLevel()).isEqualTo(serialConsistencyLevel);
		}

		if (executionProfile != null) {
			assertThat(statement.getExecutionProfileName()).isEqualTo(executionProfile);
		}
		if (keyspace != null) {
			assertThat(statement.getKeyspace()).isEqualTo(keyspace);
		}
	}
}
