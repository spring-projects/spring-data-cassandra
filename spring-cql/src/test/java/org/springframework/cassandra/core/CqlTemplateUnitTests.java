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
package org.springframework.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.cassandra.support.exception.CassandraInvalidQueryException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

/**
 * Unit tests for {@link CqlTemplate}.
 * 
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class CqlTemplateUnitTests {

	@Mock Session session;
	@Mock ResultSet resultSet;
	@Mock Row row;
	@Mock PreparedStatement preparedStatement;
	@Mock BoundStatement boundStatement;
	@Mock ColumnDefinitions columnDefinitions;

	CqlTemplate template;

	@Before
	public void setup() {

		this.template = new CqlTemplate();
		this.template.setSession(session);
	}

	// -------------------------------------------------------------------------
	// Tests dealing with a plain com.datastax.driver.core.Session
	// -------------------------------------------------------------------------

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeCallbackShouldTranslateExceptions() {

		try {
			template.execute((SessionCallback<String>) session -> {
				throw new InvalidQueryException("wrong query");
			});

			fail("Missing CassandraInvalidQueryException");
		} catch (CassandraInvalidQueryException e) {
			assertThat(e).hasMessageContaining("wrong query");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeCqlShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.execute("UPDATE user SET a = 'b';");
			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("tried for query failed");
		}
	}

	// -------------------------------------------------------------------------
	// Tests dealing with static CQL
	// -------------------------------------------------------------------------

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeCqlShouldCallExecution() {

		doTestStrings(null, null, null, cqlTemplate -> {

			cqlTemplate.execute("SELECT * from USERS");

			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeCqlWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, cqlTemplate -> {

			cqlTemplate.execute("SELECT * from USERS");

			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForResultSetShouldCallExecution() {

		doTestStrings(null, null, null, cqlTemplate -> {

			ResultSet resultSet = cqlTemplate.queryForResultSet("SELECT * from USERS");

			assertThat(resultSet).hasSize(3);
			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryWithResultSetExtractorShouldCallExecution() {

		doTestStrings(null, null, null, cqlTemplate -> {

			List<String> rows = cqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0));

			assertThat(rows).hasSize(3).contains("Walter", "Hank", " Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryWithResultSetExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, cqlTemplate -> {

			List<String> rows = cqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0));

			assertThat(rows).hasSize(3).contains("Walter", "Hank", " Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryCqlShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.query("UPDATE user SET a = 'b';", ResultSet::wasApplied);
			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("tried for query failed");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectCqlShouldBeEmpty() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		try {
			template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (EmptyResultDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 0");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectCqlShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
		assertThat(result).isEqualTo("OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectCqlShouldReturnNullValue() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject("SELECT * FROM user", (row, rowNum) -> null);
		assertThat(result).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectCqlShouldFailReturningManyRecords() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());

		try {
			template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");
			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (IncorrectResultSizeDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 2");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectCqlWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		String result = template.queryForObject("SELECT * FROM user", String.class);

		assertThat(result).isEqualTo("OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForListCqlWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		List<String> result = template.queryForList("SELECT * FROM user", String.class);

		assertThat(result).contains("OK", "NOT OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeCqlShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.wasApplied()).thenReturn(true);

		boolean applied = template.execute("UPDATE user SET a = 'b';");

		assertThat(applied).isTrue();
	}

	// -------------------------------------------------------------------------
	// Tests dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeStatementShouldCallExecution() {

		doTestStrings(null, null, null, cqlTemplate -> {

			cqlTemplate.execute(new SimpleStatement("SELECT * from USERS"));

			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeStatementWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, cqlTemplate -> {

			cqlTemplate.execute(new SimpleStatement("SELECT * from USERS"));

			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForResultStatementSetShouldCallExecution() {

		doTestStrings(null, null, null, cqlTemplate -> {

			ResultSet resultSet = cqlTemplate.queryForResultSet(new SimpleStatement("SELECT * from USERS"));

			assertThat(resultSet).hasSize(3);
			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryWithResultSetStatementExtractorShouldCallExecution() {

		doTestStrings(null, null, null, cqlTemplate -> {

			List<String> result = cqlTemplate.query(new SimpleStatement("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(result).hasSize(3).contains("Walter", "Hank", " Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryWithResultSetStatementExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, cqlTemplate -> {

			List<String> result = cqlTemplate.query(new SimpleStatement("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			assertThat(result).hasSize(3).contains("Walter", "Hank", " Jesse");
			verify(session).execute(any(Statement.class));
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryStatementShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.query(new SimpleStatement("UPDATE user SET a = 'b';"), ResultSet::wasApplied);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("tried for query failed");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectStatementShouldBeEmpty() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.emptyIterator());

		try {
			template.queryForObject(new SimpleStatement("SELECT * FROM user"), (row, rowNum) -> "OK");

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (IncorrectResultSizeDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 0");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectStatementShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject(new SimpleStatement("SELECT * FROM user"), (row, rowNum) -> "OK");
		assertThat(result).isEqualTo("OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectStatementShouldReturnNullValue() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject(new SimpleStatement("SELECT * FROM user"), (row, rowNum) -> null);
		assertThat(result).isNull();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectStatementShouldFailReturningManyRecords() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());

		try {
			template.queryForObject(new SimpleStatement("SELECT * FROM user"), (row, rowNum) -> "OK");

			fail("Missing IncorrectResultSizeDataAccessException");
		} catch (IncorrectResultSizeDataAccessException e) {
			assertThat(e).hasMessageContaining("expected 1, actual 2");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectStatementWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		String result = template.queryForObject(new SimpleStatement("SELECT * FROM user"), String.class);

		assertThat(result).isEqualTo("OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForListStatementWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		List<String> result = template.queryForList(new SimpleStatement("SELECT * FROM user"), String.class);

		assertThat(result).contains("OK", "NOT OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executeStatementShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(resultSet);
		when(resultSet.wasApplied()).thenReturn(true);

		boolean applied = template.execute(new SimpleStatement("UPDATE user SET a = 'b';"));

		assertThat(applied).isTrue();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with prepared statements
	// -------------------------------------------------------------------------

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(null, null, null, cqlTemplate -> {

			ResultSet resultSet = cqlTemplate.execute("SELECT * from USERS",
					(PreparedStatementCallback<ResultSet>) (ps) -> cqlTemplate.getSession().execute(ps.bind("A")));

			try {
				assertThat(resultSet).hasSize(3);
			} catch (Exception e) {
				fail(e.getMessage(), e);
			}
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executePreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(null, null, null, cqlTemplate -> {

			when(this.preparedStatement.bind("White")).thenReturn(this.boundStatement);
			when(this.resultSet.wasApplied()).thenReturn(true);

			boolean applied = cqlTemplate.execute("UPDATE users SET name = ?", "White");

			assertThat(applied).isTrue();
		});
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executePreparedStatementCreatorShouldTranslateStatementCreationExceptions() {

		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.wasApplied()).thenReturn(true);

		try {
			template.execute(session -> {
				throw new NoHostAvailableException(Collections.emptyMap());
			}, (ps) -> session.execute(boundStatement));

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("tried for query");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void executePreparedStatementCreatorShouldTranslateStatementCallbackExceptions() {

		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.wasApplied()).thenReturn(true);

		try {
			template.execute(session -> preparedStatement, (ps) -> {
				throw new NoHostAvailableException(Collections.emptyMap());
			});

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasMessageContaining("tried for query");
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementCreatorShouldReturnResult() {

		when(session.prepare(anyString())).thenReturn(preparedStatement);
		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		Iterator<Row> iterator = template.query(session -> preparedStatement, ResultSet::iterator);

		assertThat(iterator).hasSize(1).contains(row);
		verify(preparedStatement).bind();
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementCreatorAndBinderShouldReturnResult() {

		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		ResultSet resultSet = template.query(session -> preparedStatement, ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, rs -> rs);

		assertThat(resultSet).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementCreatorAndBinderShouldTranslatePrepareStatementExceptions() {

		when(preparedStatement.bind()).thenReturn(boundStatement);

		try {
			template.query(session -> {
				throw new NoHostAvailableException(Collections.emptyMap());
			}, ps -> {
				ps.bind("a", "b");
				return boundStatement;
			}, rs -> rs);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementCreatorAndBinderShouldTranslateBindExceptions() {

		when(preparedStatement.bind()).thenReturn(boundStatement);

		try {
			template.query(session -> preparedStatement, ps -> {
				throw new NoHostAvailableException(Collections.emptyMap());
			}, rs -> rs);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementCreatorAndBinderShouldTranslateExecutionExceptions() {

		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		try {
			template.query(session -> preparedStatement, ps -> {
				ps.bind("a", "b");
				return boundStatement;
			}, rs -> rs);

			fail("Missing CassandraConnectionFailureException");
		} catch (CassandraConnectionFailureException e) {
			assertThat(e).hasCauseInstanceOf(NoHostAvailableException.class);
		}
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryPreparedStatementCreatorAndBinderAndMapperShouldReturnResult() {

		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		List<Row> rows = template.query(session -> preparedStatement, ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, (row, rowNum) -> row);

		assertThat(rows).hasSize(1).contains(row);
		verify(preparedStatement).bind("a", "b");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectPreparedStatementShouldBeEmpty() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(preparedStatement);
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

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectPreparedStatementShouldReturnRecord() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

		String result = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK", "Walter");
		assertThat(result).isEqualTo("OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectPreparedStatementShouldFailReturningManyRecords() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(preparedStatement);
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

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForObjectPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		String result = template.queryForObject("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		assertThat(result).isEqualTo("OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void queryForListPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		List<String> result = template.queryForList("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		assertThat(result).contains("OK", "NOT OK");
	}

	/**
	 * @see DATACASS-292
	 */
	@Test
	public void updatePreparedStatementShouldReturnApplied() {

		when(session.prepare("UPDATE user SET username = ?")).thenReturn(preparedStatement);
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(resultSet);
		when(resultSet.wasApplied()).thenReturn(true);

		boolean applied = template.execute("UPDATE user SET username = ?", "Walter");

		assertThat(applied).isTrue();
	}


	private <T> void doTestStrings(Integer fetchSize, ConsistencyLevel consistencyLevel,
			com.datastax.driver.core.policies.RetryPolicy retryPolicy, Consumer<CqlTemplate> cqlTemplateConsumer) {

		String[] results = { "Walter", "Hank", " Jesse" };

		when(this.session.execute(any(Statement.class))).thenReturn(resultSet);
		when(this.resultSet.iterator()).thenReturn(Arrays.asList(row, row, row).iterator());

		when(this.row.getString(0)).thenReturn(results[0], results[1], results[2]);
		when(this.session.prepare(anyString())).thenReturn(preparedStatement);

		CqlTemplate template = new CqlTemplate();
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
		verify(this.session).execute(statementArgumentCaptor.capture());

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
}
