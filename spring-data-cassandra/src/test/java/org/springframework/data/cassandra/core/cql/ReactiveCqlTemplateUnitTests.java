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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

/**
 * Unit tests for {@link ReactiveCqlTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveCqlTemplateUnitTests {

	@Mock ReactiveSession session;
	@Mock ReactiveResultSet reactiveResultSet;
	@Mock Row row;
	@Mock PreparedStatement preparedStatement;
	@Mock BoundStatement boundStatement;
	@Mock ColumnDefinitions columnDefinitions;

	ReactiveCqlTemplate template;
	ReactiveSessionFactory sessionFactory;

	@Before
	public void setup() throws Exception {

		this.sessionFactory = new DefaultReactiveSessionFactory(session);
		this.template = new ReactiveCqlTemplate(sessionFactory);
	}

	// -------------------------------------------------------------------------
	// Tests dealing with a plain org.springframework.data.cassandra.core.cql.ReactiveSession
	// -------------------------------------------------------------------------

	@Test // DATACASS-335
	public void executeCallbackShouldExecuteDeferred() {

		Flux<String> flux = template.execute((ReactiveSessionCallback<String>) session -> {
			session.close();
			return Mono.just("OK");
		});

		verify(session, never()).close();

		StepVerifier.create(flux).expectNext("OK").verifyComplete();
		verify(session).close();
	}

	@Test // DATACASS-335
	public void executeCallbackShouldTranslateExceptions() {

		Flux<String> flux = template.execute((ReactiveSessionCallback<String>) session -> {
			throw new InvalidQueryException("wrong query");
		});

		StepVerifier.create(flux).expectError(CassandraInvalidQueryException.class).verify();
	}

	@Test // DATACASS-335
	public void executeCqlShouldExecuteDeferred() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));

		Mono<Boolean> mono = template.execute("UPDATE user SET a = 'b';");

		verifyZeroInteractions(session);

		StepVerifier.create(mono).expectNext(false).verifyComplete();

		verify(session).execute(any(Statement.class));
	}

	@Test // DATACASS-335
	public void executeCqlShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		Mono<Boolean> mono = template.execute("UPDATE user SET a = 'b';");

		StepVerifier.create(mono).expectError(CassandraConnectionFailureException.class).verify();
	}

	// -------------------------------------------------------------------------
	// Tests dealing with static CQL
	// -------------------------------------------------------------------------

	@Test // DATACASS-335
	public void executeCqlShouldCallExecution() {

		doTestStrings(null, null, null, reactiveCqlTemplate -> {

			StepVerifier.create(reactiveCqlTemplate.execute("SELECT * from USERS")).expectNextCount(1).verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void executeCqlWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, reactiveCqlTemplate -> {

			StepVerifier.create(reactiveCqlTemplate.execute("SELECT * from USERS")) //
					.expectNextCount(1) //
					.verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void queryForResultSetShouldCallExecution() {

		doTestStrings(null, null, null, reactiveCqlTemplate -> {

			Mono<ReactiveResultSet> mono = reactiveCqlTemplate.queryForResultSet("SELECT * from USERS");

			StepVerifier.create(mono.flatMapMany(ReactiveResultSet::rows)).expectNextCount(3).verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void queryWithResultSetExtractorShouldCallExecution() {

		doTestStrings(null, null, null, reactiveCqlTemplate -> {

			Flux<String> flux = reactiveCqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0));

			StepVerifier.create(flux).expectNext("Walter", "Hank", " Jesse").verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void queryWithResultSetExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, reactiveCqlTemplate -> {

			Flux<String> flux = reactiveCqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0));

			StepVerifier.create(flux).expectNext("Walter", "Hank", " Jesse").verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void queryCqlShouldExecuteDeferred() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));

		Flux<Boolean> flux = template.query("UPDATE user SET a = 'b';", resultSet -> Mono.just(resultSet.wasApplied()));

		verifyZeroInteractions(session);

		StepVerifier.create(flux).expectNext(true).verifyComplete();

		verify(session).execute(any(Statement.class));
	}

	@Test // DATACASS-335
	public void queryCqlShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		Flux<Boolean> flux = template.query("UPDATE user SET a = 'b';", resultSet -> Mono.just(resultSet.wasApplied()));

		StepVerifier.create(flux).expectError(CassandraConnectionFailureException.class).verify();
	}

	@Test // DATACASS-335
	public void queryForObjectCqlShouldBeEmpty() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		Mono<String> mono = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");

		StepVerifier.create(mono).verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectCqlShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");

		StepVerifier.create(mono).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectCqlShouldReturnNullValue() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user", (row, rowNum) -> null);

		StepVerifier.create(mono).verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectCqlShouldFailReturningManyRecords() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");

		StepVerifier.create(mono).expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-335
	public void queryForObjectCqlWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		Mono<String> mono = template.queryForObject("SELECT * FROM user", String.class);

		StepVerifier.create(mono).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForFluxCqlWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		Flux<String> flux = template.queryForFlux("SELECT * FROM user", String.class);

		StepVerifier.create(flux).expectNext("OK", "NOT OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForRowsCqlReturnRows() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Flux<Row> flux = template.queryForRows("SELECT * FROM user");

		StepVerifier.create(flux).expectNext(row, row).verifyComplete();
	}

	@Test // DATACASS-335
	public void executeCqlShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Mono<Boolean> mono = template.execute("UPDATE user SET a = 'b';");

		StepVerifier.create(mono).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	public void executeCqlPublisherShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true, false);

		Flux<Boolean> flux = template.execute(Flux.just("UPDATE user SET a = 'b';", "UPDATE user SET x = 'y';"));

		verifyZeroInteractions(session);

		StepVerifier.create(flux).expectNext(true).expectNext(false).verifyComplete();

		verify(session, times(2)).execute(any(Statement.class));
	}

	// -------------------------------------------------------------------------
	// Tests dealing with com.datastax.driver.core.Statement
	// -------------------------------------------------------------------------

	@Test // DATACASS-335
	public void executeStatementShouldCallExecution() {

		doTestStrings(null, null, null, reactiveCqlTemplate -> {

			StepVerifier.create(reactiveCqlTemplate.execute(new SimpleStatement("SELECT * from USERS"))) //
					.expectNextCount(1) //
					.verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void executeStatementWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, reactiveCqlTemplate -> {

			StepVerifier.create(reactiveCqlTemplate.execute(new SimpleStatement("SELECT * from USERS"))) //
					.expectNextCount(1) //
					.verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void queryForResultStatementSetShouldCallExecution() {

		doTestStrings(null, null, null, reactiveCqlTemplate -> {

			StepVerifier
					.create(reactiveCqlTemplate.queryForResultSet(new SimpleStatement("SELECT * from USERS"))
							.flatMapMany(ReactiveResultSet::rows)) //
					.expectNextCount(3) //
					.verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void queryWithResultSetStatementExtractorShouldCallExecution() {

		doTestStrings(null, null, null, reactiveCqlTemplate -> {

			Flux<String> flux = reactiveCqlTemplate.query(new SimpleStatement("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			StepVerifier.create(flux).expectNext("Walter", "Hank", " Jesse").verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void queryWithResultSetStatementExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DowngradingConsistencyRetryPolicy.INSTANCE, reactiveCqlTemplate -> {

			Flux<String> flux = reactiveCqlTemplate.query(new SimpleStatement("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			StepVerifier.create(flux.collectList()).consumeNextWith(rows -> {

				assertThat(rows).hasSize(3).contains("Walter", "Hank", " Jesse");
			}).verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	public void queryStatementShouldExecuteDeferred() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));

		Flux<Boolean> flux = template.query(new SimpleStatement("UPDATE user SET a = 'b';"),
				resultSet -> Mono.just(resultSet.wasApplied()));

		verifyZeroInteractions(session);
		StepVerifier.create(flux).expectNext(true).verifyComplete();
		verify(session).execute(any(Statement.class));
	}

	@Test // DATACASS-335
	public void queryStatementShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoHostAvailableException(Collections.emptyMap()));

		Flux<Boolean> flux = template.query(new SimpleStatement("UPDATE user SET a = 'b';"),
				resultSet -> Mono.just(resultSet.wasApplied()));

		StepVerifier.create(flux).expectError(CassandraConnectionFailureException.class).verify();
	}

	@Test // DATACASS-335
	public void queryForObjectStatementShouldBeEmpty() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		Mono<String> mono = template.queryForObject(new SimpleStatement("SELECT * FROM user"), (row, rowNum) -> "OK");

		StepVerifier.create(mono).verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectStatementShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject(new SimpleStatement("SELECT * FROM user"), (row, rowNum) -> "OK");

		StepVerifier.create(mono).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectStatementShouldReturnNullValue() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject(new SimpleStatement("SELECT * FROM user"), (row, rowNum) -> null);

		StepVerifier.create(mono).verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectStatementShouldFailReturningManyRecords() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Mono<String> mono = template.queryForObject(new SimpleStatement("SELECT * FROM user"), (row, rowNum) -> "OK");

		StepVerifier.create(mono).expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-335
	public void queryForObjectStatementWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		Mono<String> mono = template.queryForObject(new SimpleStatement("SELECT * FROM user"), String.class);

		StepVerifier.create(mono).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForFluxStatementWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		Flux<String> flux = template.queryForFlux(new SimpleStatement("SELECT * FROM user"), String.class);

		StepVerifier.create(flux).expectNext("OK", "NOT OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForRowsStatementReturnRows() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Flux<Row> flux = template.queryForRows(new SimpleStatement("SELECT * FROM user"));

		StepVerifier.create(flux).expectNext(row, row).verifyComplete();
	}

	@Test // DATACASS-335
	public void executeStatementShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true);

		StepVerifier.create(template.execute(new SimpleStatement("UPDATE user SET a = 'b';"))).expectNext(true)
				.verifyComplete();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with prepared statements
	// -------------------------------------------------------------------------

	@Test // DATACASS-335
	public void queryPreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(null, null, null, reactiveCqlTemplate -> {

			Flux<Row> flux = reactiveCqlTemplate.execute("SELECT * from USERS", (session, ps) -> {

				return session.execute(ps.bind("A")).flatMapMany(ReactiveResultSet::rows);
			});

			StepVerifier.create(flux).expectNextCount(3).verifyComplete();
		});
	}

	@Test // DATACASS-335
	public void executePreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(null, null, null, reactiveCqlTemplate -> {

			Mono<Boolean> applied = reactiveCqlTemplate.execute("UPDATE users SET name = ?", "White");
			when(this.preparedStatement.bind("White")).thenReturn(this.boundStatement);
			when(this.reactiveResultSet.wasApplied()).thenReturn(true);

			StepVerifier.create(applied).expectNext(true).verifyComplete();
		});
	}

	@Test // DATACASS-335
	public void executePreparedStatementCallbackShouldExecuteDeferred() {

		when(session.prepare(anyString())).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));

		Flux<ReactiveResultSet> flux = template.execute("UPDATE user SET a = 'b';",
				(session, ps) -> session.execute(ps.bind()));

		verifyZeroInteractions(session);

		StepVerifier.create(flux).expectNext(reactiveResultSet).verifyComplete();

		verify(session).prepare(anyString());
		verify(session).execute(boundStatement);
	}

	@Test // DATACASS-335
	public void executePreparedStatementCreatorShouldExecuteDeferred() {

		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));

		Flux<ReactiveResultSet> flux = template.execute(session -> Mono.just(preparedStatement),
				(session, ps) -> session.execute(boundStatement));

		verifyZeroInteractions(session);

		StepVerifier.create(flux).expectNext(reactiveResultSet).verifyComplete();

		verify(session).execute(boundStatement);
	}

	@Test // DATACASS-335
	public void executePreparedStatementCreatorShouldTranslateStatementCreationExceptions() {

		Flux<ReactiveResultSet> flux = template.execute(session -> {
			throw new NoHostAvailableException(Collections.emptyMap());
		}, (session, ps) -> session.execute(boundStatement));

		StepVerifier.create(flux).expectError(CassandraConnectionFailureException.class).verify();
	}

	@Test // DATACASS-335
	public void executePreparedStatementCreatorShouldTranslateStatementCallbackExceptions() {

		Flux<ReactiveResultSet> flux = template.execute(session -> Mono.just(preparedStatement), (session, ps) -> {
			throw new NoHostAvailableException(Collections.emptyMap());
		});

		StepVerifier.create(flux).expectError(CassandraConnectionFailureException.class).verify();
	}

	@Test // DATACASS-335
	public void queryPreparedStatementCreatorShouldReturnResult() {

		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Flux<Row> flux = template.query(session -> Mono.just(preparedStatement), ReactiveResultSet::rows);

		verifyZeroInteractions(session);

		StepVerifier.create(flux).expectNext(row).verifyComplete();
		verify(preparedStatement).bind();
	}

	@Test // DATACASS-335
	public void queryPreparedStatementCreatorAndBinderShouldReturnResult() {

		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Flux<Row> flux = template.query(session -> Mono.just(preparedStatement), ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, ReactiveResultSet::rows);

		verifyZeroInteractions(session);

		StepVerifier.create(flux).expectNext(row).verifyComplete();

		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-335
	public void queryPreparedStatementCreatorAndBinderAndMapperShouldReturnResult() {

		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Flux<Row> flux = template.query(session -> Mono.just(preparedStatement), ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, (row, rowNum) -> row);

		verifyZeroInteractions(session);

		StepVerifier.create(flux).expectNext(row).verifyComplete();

		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-335
	public void queryForObjectPreparedStatementShouldBeEmpty() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		Mono<String> mono = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK",
				"Walter");

		StepVerifier.create(mono).verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectPreparedStatementShouldReturnRecord() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK",
				"Walter");

		StepVerifier.create(mono).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForObjectPreparedStatementShouldFailReturningManyRecords() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK",
				"Walter");

		StepVerifier.create(mono).expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-335
	public void queryForObjectPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		Mono<String> mono = template.queryForObject("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		StepVerifier.create(mono).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForFluxPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		Flux<String> flux = template.queryForFlux("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		StepVerifier.create(flux).expectNext("OK", "NOT OK").verifyComplete();
	}

	@Test // DATACASS-335
	public void queryForRowsPreparedStatementReturnRows() {

		when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Flux<Row> flux = template.queryForRows("SELECT * FROM user WHERE username = ?", "Walter");

		StepVerifier.create(flux).expectNextCount(2).verifyComplete();
	}

	@Test // DATACASS-335
	public void updatePreparedStatementShouldReturnApplied() {

		when(session.prepare("UPDATE user SET username = ?")).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Mono<Boolean> mono = template.execute("UPDATE user SET username = ?", "Walter");

		StepVerifier.create(mono).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	public void updatePreparedStatementArgsPublisherShouldReturnApplied() {

		when(session.prepare("UPDATE user SET username = ?")).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(preparedStatement.bind("Hank")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Flux<Boolean> flux = template.execute("UPDATE user SET username = ?",
				Flux.just(new Object[] { "Walter" }, new Object[] { "Hank" }));

		StepVerifier.create(flux).expectNext(true, true).verifyComplete();

		verify(session, atMost(1)).prepare("UPDATE user SET username = ?");
		verify(session, times(2)).execute(boundStatement);
	}

	private <T> void doTestStrings(Integer fetchSize, com.datastax.driver.core.ConsistencyLevel consistencyLevel,
			com.datastax.driver.core.policies.RetryPolicy retryPolicy, Consumer<ReactiveCqlTemplate> cqlTemplateConsumer) {

		String[] results = { "Walter", "Hank", " Jesse" };

		when(this.session.execute((Statement) any())).thenReturn(Mono.just(reactiveResultSet));
		when(this.reactiveResultSet.rows()).thenReturn(Flux.just(row, row, row));

		when(this.row.getString(0)).thenReturn(results[0], results[1], results[2]);
		when(this.session.prepare(anyString())).thenReturn(Mono.just(this.preparedStatement));

		ReactiveCqlTemplate template = new ReactiveCqlTemplate();
		template.setSessionFactory(this.sessionFactory);

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
