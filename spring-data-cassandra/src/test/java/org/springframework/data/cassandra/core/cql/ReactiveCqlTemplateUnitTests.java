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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.session.DefaultReactiveSessionFactory;
import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;

/**
 * Unit tests for {@link ReactiveCqlTemplate}.
 *
 * @author Mark Paluch
 * @author Tomasz Lelek
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveCqlTemplateUnitTests {

	@Mock ReactiveSession session;
	@Mock ReactiveResultSet reactiveResultSet;
	@Mock Row row;
	@Mock PreparedStatement preparedStatement;
	@Mock BoundStatement boundStatement;
	@Mock ColumnDefinitions columnDefinitions;

	private ReactiveCqlTemplate template;
	private ReactiveSessionFactory sessionFactory;

	@BeforeEach
	void setup() throws Exception {

		this.sessionFactory = new DefaultReactiveSessionFactory(session);
		this.template = new ReactiveCqlTemplate(sessionFactory);
	}

	// -------------------------------------------------------------------------
	// Tests dealing with a plain org.springframework.data.cassandra.core.cql.ReactiveSession
	// -------------------------------------------------------------------------

	@Test // DATACASS-335
	void executeCallbackShouldExecuteDeferred() {

		Flux<String> flux = template.execute((ReactiveSessionCallback<String>) session -> {
			session.close();
			return Mono.just("OK");
		});

		verify(session, never()).close();

		flux.as(StepVerifier::create).expectNext("OK").verifyComplete();
		verify(session).close();
	}

	@Test // DATACASS-335
	void executeCallbackShouldTranslateExceptions() {

		Flux<String> flux = template.execute((ReactiveSessionCallback<String>) session -> {
			throw new InvalidQueryException(null, "wrong query");
		});

		flux.as(StepVerifier::create).expectError(CassandraInvalidQueryException.class).verify();
	}

	@Test // DATACASS-335
	void executeCqlShouldExecuteDeferred() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));

		Mono<Boolean> mono = template.execute("UPDATE user SET a = 'b';");

		verifyZeroInteractions(session);

		mono.as(StepVerifier::create).expectNext(false).verifyComplete();

		verify(session).execute(any(Statement.class));
	}

	@Test // DATACASS-335
	void executeCqlShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		Mono<Boolean> mono = template.execute("UPDATE user SET a = 'b';");

		mono.as(StepVerifier::create).expectError(CassandraConnectionFailureException.class).verify();
	}

	// -------------------------------------------------------------------------
	// Tests dealing with static CQL
	// -------------------------------------------------------------------------

	@Test // DATACASS-335
	void executeCqlShouldCallExecution() {

		doTestStrings(reactiveCqlTemplate -> {

			reactiveCqlTemplate.execute("SELECT * from USERS").as(StepVerifier::create).expectNextCount(1).verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void executeCqlWithArgumentsShouldCallExecution() {

		doTestStrings(5, DefaultConsistencyLevel.ONE, null, "foo", reactiveCqlTemplate -> {

			reactiveCqlTemplate.execute("SELECT * from USERS").as(StepVerifier::create) //
					.expectNextCount(1) //
					.verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void queryForResultSetShouldCallExecution() {

		doTestStrings(reactiveCqlTemplate -> {

			Mono<ReactiveResultSet> mono = reactiveCqlTemplate.queryForResultSet("SELECT * from USERS");

			mono.flatMapMany(ReactiveResultSet::rows).as(StepVerifier::create).expectNextCount(3).verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void queryWithResultSetExtractorShouldCallExecution() {

		doTestStrings(reactiveCqlTemplate -> {

			Flux<String> flux = reactiveCqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0));

			flux.as(StepVerifier::create).expectNext("Walter", "Hank", "Jesse").verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void queryWithResultSetExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, "foo", reactiveCqlTemplate -> {

			Flux<String> flux = reactiveCqlTemplate.query("SELECT * from USERS", (row, index) -> row.getString(0));

			flux.as(StepVerifier::create).expectNext("Walter", "Hank", "Jesse").verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void queryCqlShouldExecuteDeferred() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));

		Flux<Boolean> flux = template.query("UPDATE user SET a = 'b';", resultSet -> Mono.just(resultSet.wasApplied()));

		verifyZeroInteractions(session);

		flux.as(StepVerifier::create).expectNext(true).verifyComplete();

		verify(session).execute(any(Statement.class));
	}

	@Test // DATACASS-335
	void queryCqlShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		Flux<Boolean> flux = template.query("UPDATE user SET a = 'b';", resultSet -> Mono.just(resultSet.wasApplied()));

		flux.as(StepVerifier::create).expectError(CassandraConnectionFailureException.class).verify();
	}

	@Test // DATACASS-335
	void queryForObjectCqlShouldBeEmpty() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		Mono<String> mono = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");

		mono.as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectCqlShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");

		mono.as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectCqlShouldReturnNullValue() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user", (row, rowNum) -> null);

		mono.as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectCqlShouldFailReturningManyRecords() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user", (row, rowNum) -> "OK");

		mono.as(StepVerifier::create).expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-335
	void queryForObjectCqlWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		Mono<String> mono = template.queryForObject("SELECT * FROM user", String.class);

		mono.as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForFluxCqlWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		Flux<String> flux = template.queryForFlux("SELECT * FROM user", String.class);

		flux.as(StepVerifier::create).expectNext("OK", "NOT OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForRowsCqlReturnRows() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Flux<Row> flux = template.queryForRows("SELECT * FROM user");

		flux.as(StepVerifier::create).expectNext(row, row).verifyComplete();
	}

	@Test // DATACASS-335
	void executeCqlShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Mono<Boolean> mono = template.execute("UPDATE user SET a = 'b';");

		mono.as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	void executeCqlPublisherShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true, false);

		Flux<Boolean> flux = template.execute(Flux.just("UPDATE user SET a = 'b';", "UPDATE user SET x = 'y';"));

		verifyZeroInteractions(session);

		flux.as(StepVerifier::create).expectNext(true).expectNext(false).verifyComplete();

		verify(session, times(2)).execute(any(Statement.class));
	}

	// -------------------------------------------------------------------------
	// Tests dealing with com.datastax.oss.driver.api.core.cql.Statement
	// -------------------------------------------------------------------------

	@Test // DATACASS-335
	void executeStatementShouldCallExecution() {

		doTestStrings(reactiveCqlTemplate -> {

			reactiveCqlTemplate.execute(SimpleStatement.newInstance("SELECT * from USERS")).as(StepVerifier::create) //
					.expectNextCount(1) //
					.verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void executeStatementWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, DefaultConsistencyLevel.EACH_QUORUM, "foo", reactiveCqlTemplate -> {

			reactiveCqlTemplate.execute(SimpleStatement.newInstance("SELECT * from USERS")).as(StepVerifier::create) //
					.expectNextCount(1) //
					.verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void queryForResultStatementSetShouldCallExecution() {

		doTestStrings(reactiveCqlTemplate -> {

			reactiveCqlTemplate.queryForResultSet(SimpleStatement.newInstance("SELECT * from USERS"))
					.flatMapMany(ReactiveResultSet::rows).as(StepVerifier::create) //
					.expectNextCount(3) //
					.verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void queryWithResultSetStatementExtractorShouldCallExecution() {

		doTestStrings(reactiveCqlTemplate -> {

			Flux<String> flux = reactiveCqlTemplate.query(SimpleStatement.newInstance("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			flux.as(StepVerifier::create).expectNext("Walter", "Hank", "Jesse").verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void queryWithResultSetStatementExtractorWithArgumentsShouldCallExecution() {

		doTestStrings(5, ConsistencyLevel.ONE, null, "foo", reactiveCqlTemplate -> {

			Flux<String> flux = reactiveCqlTemplate.query(SimpleStatement.newInstance("SELECT * from USERS"),
					(row, index) -> row.getString(0));

			flux.collectList().as(StepVerifier::create).consumeNextWith(rows -> {

				assertThat(rows).hasSize(3).contains("Walter", "Hank", "Jesse");
			}).verifyComplete();

			verify(session).execute(any(Statement.class));
		});
	}

	@Test // DATACASS-335
	void queryStatementShouldExecuteDeferred() {

		when(reactiveResultSet.wasApplied()).thenReturn(true);
		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));

		Flux<Boolean> flux = template.query(SimpleStatement.newInstance("UPDATE user SET a = 'b';"),
				resultSet -> Mono.just(resultSet.wasApplied()));

		verifyZeroInteractions(session);
		flux.as(StepVerifier::create).expectNext(true).verifyComplete();
		verify(session).execute(any(Statement.class));
	}

	@Test // DATACASS-335
	void queryStatementShouldTranslateExceptions() {

		when(session.execute(any(Statement.class))).thenThrow(new NoNodeAvailableException());

		Flux<Boolean> flux = template.query(SimpleStatement.newInstance("UPDATE user SET a = 'b';"),
				resultSet -> Mono.just(resultSet.wasApplied()));

		flux.as(StepVerifier::create).expectError(CassandraConnectionFailureException.class).verify();
	}

	@Test // DATACASS-335
	void queryForObjectStatementShouldBeEmpty() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		Mono<String> mono = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
				(row, rowNum) -> "OK");

		mono.as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectStatementShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
				(row, rowNum) -> "OK");

		mono.as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectStatementShouldReturnNullValue() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
				(row, rowNum) -> null);

		mono.as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectStatementShouldFailReturningManyRecords() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Mono<String> mono = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"),
				(row, rowNum) -> "OK");

		mono.as(StepVerifier::create).expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-335
	void queryForObjectStatementWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		Mono<String> mono = template.queryForObject(SimpleStatement.newInstance("SELECT * FROM user"), String.class);

		mono.as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForFluxStatementWithTypeShouldReturnRecord() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		Flux<String> flux = template.queryForFlux(SimpleStatement.newInstance("SELECT * FROM user"), String.class);

		flux.as(StepVerifier::create).expectNext("OK", "NOT OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForRowsStatementReturnRows() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Flux<Row> flux = template.queryForRows(SimpleStatement.newInstance("SELECT * FROM user"));

		flux.as(StepVerifier::create).expectNext(row, row).verifyComplete();
	}

	@Test // DATACASS-335
	void executeStatementShouldReturnWasApplied() {

		when(session.execute(any(Statement.class))).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true);

		template.execute(SimpleStatement.newInstance("UPDATE user SET a = 'b';")).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with prepared statements
	// -------------------------------------------------------------------------

	@Test // DATACASS-335
	void queryPreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(reactiveCqlTemplate -> {

			Flux<Row> flux = reactiveCqlTemplate.execute("SELECT * from USERS", (session, ps) -> {

				return session.execute(ps.bind("A")).flatMapMany(ReactiveResultSet::rows);
			});

			flux.as(StepVerifier::create).expectNextCount(3).verifyComplete();
		});
	}

	@Test // DATACASS-767
	void executePreparedStatementShouldApplyKeyspace() {

		when(preparedStatement.bind("A")).thenReturn(boundStatement);
		when(boundStatement.getKeyspace()).thenReturn(CqlIdentifier.fromCql("ks1"));

		doTestStrings(null, null, null, null, CqlIdentifier.fromCql("ks1"), cqlTemplate -> {
			cqlTemplate.execute("SELECT * from USERS", (session, ps) -> session.execute(ps.bind("A")))
					.as(StepVerifier::create).expectNextCount(1).verifyComplete();
		});

		ArgumentCaptor<SimpleStatement> captor = ArgumentCaptor.forClass(SimpleStatement.class);
		verify(session).prepare(captor.capture());

		SimpleStatement statement = captor.getValue();
		assertThat(statement.getKeyspace()).isEqualTo(CqlIdentifier.fromCql("ks1"));
	}

	@Test // DATACASS-335
	void executePreparedStatementWithCallbackShouldCallExecution() {

		doTestStrings(reactiveCqlTemplate -> {

			Mono<Boolean> applied = reactiveCqlTemplate.execute("UPDATE users SET name = ?", "White");
			when(this.preparedStatement.bind("White")).thenReturn(this.boundStatement);
			when(this.reactiveResultSet.wasApplied()).thenReturn(true);

			applied.as(StepVerifier::create).expectNext(true).verifyComplete();
		});
	}

	@Test // DATACASS-335
	void executePreparedStatementCallbackShouldExecuteDeferred() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));

		Flux<ReactiveResultSet> flux = template.execute("UPDATE user SET a = 'b';",
				(session, ps) -> session.execute(ps.bind()));

		verifyZeroInteractions(session);

		flux.as(StepVerifier::create).expectNext(reactiveResultSet).verifyComplete();

		verify(session).prepare(any(SimpleStatement.class));
		verify(session).execute(boundStatement);
	}

	@Test // DATACASS-335
	void executePreparedStatementCreatorShouldExecuteDeferred() {

		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));

		Flux<ReactiveResultSet> flux = template.execute(session -> Mono.just(preparedStatement),
				(session, ps) -> session.execute(boundStatement));

		verifyZeroInteractions(session);

		flux.as(StepVerifier::create).expectNext(reactiveResultSet).verifyComplete();

		verify(session).execute(boundStatement);
	}

	@Test // DATACASS-335
	void executePreparedStatementCreatorShouldTranslateStatementCreationExceptions() {

		Flux<ReactiveResultSet> flux = template.execute(session -> {
			throw new NoNodeAvailableException();
		}, (session, ps) -> session.execute(boundStatement));

		flux.as(StepVerifier::create).expectError(CassandraConnectionFailureException.class).verify();
	}

	@Test // DATACASS-335
	void executePreparedStatementCreatorShouldTranslateStatementCallbackExceptions() {

		Flux<ReactiveResultSet> flux = template.execute(session -> Mono.just(preparedStatement), (session, ps) -> {
			throw new NoNodeAvailableException();
		});

		flux.as(StepVerifier::create).expectError(CassandraConnectionFailureException.class).verify();
	}

	@Test // DATACASS-335
	void queryPreparedStatementCreatorShouldReturnResult() {

		when(preparedStatement.bind()).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Flux<Row> flux = template.query(session -> Mono.just(preparedStatement), ReactiveResultSet::rows);

		verifyZeroInteractions(session);

		flux.as(StepVerifier::create).expectNext(row).verifyComplete();
		verify(preparedStatement).bind();
	}

	@Test // DATACASS-335
	void queryPreparedStatementCreatorAndBinderShouldReturnResult() {

		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Flux<Row> flux = template.query(session -> Mono.just(preparedStatement), ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, ReactiveResultSet::rows);

		verifyZeroInteractions(session);

		flux.as(StepVerifier::create).expectNext(row).verifyComplete();

		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-335
	void queryPreparedStatementCreatorAndBinderAndMapperShouldReturnResult() {

		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Flux<Row> flux = template.query(session -> Mono.just(preparedStatement), ps -> {
			ps.bind("a", "b");
			return boundStatement;
		}, (row, rowNum) -> row);

		verifyZeroInteractions(session);

		flux.as(StepVerifier::create).expectNext(row).verifyComplete();

		verify(preparedStatement).bind("a", "b");
	}

	@Test // DATACASS-335
	void queryForObjectPreparedStatementShouldBeEmpty() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.empty());

		Mono<String> mono = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK",
				"Walter");

		mono.as(StepVerifier::create).verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectPreparedStatementShouldReturnRecord() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK",
				"Walter");

		mono.as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForObjectPreparedStatementShouldFailReturningManyRecords() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Mono<String> mono = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK",
				"Walter");

		mono.as(StepVerifier::create).expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATACASS-335
	void queryForObjectPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK");

		Mono<String> mono = template.queryForObject("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		mono.as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForFluxPreparedStatementWithTypeShouldReturnRecord() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));
		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		when(columnDefinitions.size()).thenReturn(1);
		when(row.getString(0)).thenReturn("OK", "NOT OK");

		Flux<String> flux = template.queryForFlux("SELECT * FROM user WHERE username = ?", String.class, "Walter");

		flux.as(StepVerifier::create).expectNext("OK", "NOT OK").verifyComplete();
	}

	@Test // DATACASS-335
	void queryForRowsPreparedStatementReturnRows() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.rows()).thenReturn(Flux.just(row, row));

		Flux<Row> flux = template.queryForRows("SELECT * FROM user WHERE username = ?", "Walter");

		flux.as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test // DATACASS-335
	void updatePreparedStatementShouldReturnApplied() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Mono<Boolean> mono = template.execute("UPDATE user SET username = ?", "Walter");

		mono.as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATACASS-335
	void updatePreparedStatementArgsPublisherShouldReturnApplied() {

		when(session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(preparedStatement));
		when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
		when(preparedStatement.bind("Hank")).thenReturn(boundStatement);
		when(session.execute(boundStatement)).thenReturn(Mono.just(reactiveResultSet));
		when(reactiveResultSet.wasApplied()).thenReturn(true);

		Flux<Boolean> flux = template.execute("UPDATE user SET username = ?",
				Flux.just(new Object[] { "Walter" }, new Object[] { "Hank" }));

		flux.as(StepVerifier::create).expectNext(true, true).verifyComplete();

		verify(session, atMost(1)).prepare("UPDATE user SET username = ?");
		verify(session, times(2)).execute(boundStatement);
	}

	private void doTestStrings(Consumer<ReactiveCqlTemplate> cqlTemplateConsumer) {
		doTestStrings(null, null, null, null, null, cqlTemplateConsumer);
	}

	private void doTestStrings(@Nullable Integer fetchSize, @Nullable ConsistencyLevel consistencyLevel,
			@Nullable ConsistencyLevel serialConsistencyLevel, @Nullable String executionProfile,
			Consumer<ReactiveCqlTemplate> cqlTemplateConsumer) {
		doTestStrings(fetchSize, consistencyLevel, serialConsistencyLevel, executionProfile, null, cqlTemplateConsumer);
	}

	private void doTestStrings(@Nullable Integer fetchSize, @Nullable ConsistencyLevel consistencyLevel,
			@Nullable ConsistencyLevel serialConsistencyLevel, @Nullable String executionProfile,
			@Nullable CqlIdentifier keyspace, Consumer<ReactiveCqlTemplate> cqlTemplateConsumer) {

		String[] results = { "Walter", "Hank", "Jesse" };

		when(this.session.execute((Statement) any())).thenReturn(Mono.just(reactiveResultSet));
		when(this.reactiveResultSet.rows()).thenReturn(Flux.just(row, row, row));

		when(this.row.getString(0)).thenReturn(results[0], results[1], results[2]);
		when(this.session.prepare(anyString())).thenReturn(Mono.just(this.preparedStatement));
		when(this.session.prepare(any(SimpleStatement.class))).thenReturn(Mono.just(this.preparedStatement));

		ReactiveCqlTemplate template = new ReactiveCqlTemplate();
		template.setSessionFactory(this.sessionFactory);

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
