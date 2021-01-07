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
import static org.mockito.Mockito.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;
import org.springframework.scheduling.annotation.AsyncResult;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Unit tests for {@link DefaultBridgedReactiveSession}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultBridgedReactiveSessionUnitTests {

	@Mock CqlSession sessionMock;

	private CompletableFuture<AsyncResultSet> future = new CompletableFuture<>();
	private CompletableFuture<PreparedStatement> preparedStatementFuture = new CompletableFuture<>();

	private DefaultBridgedReactiveSession reactiveSession;

	@BeforeEach
	void before() {

		reactiveSession = new DefaultBridgedReactiveSession(sessionMock);

		when(sessionMock.executeAsync(any(Statement.class))).thenReturn(future);
	}

	@Test // DATACASS-335
	void executeStatementShouldForwardStatementToSession() {

		Statement<?> statement = SimpleStatement.newInstance("SELECT *");

		reactiveSession.execute(statement).subscribe();

		verify(sessionMock).executeAsync(statement);
	}

	@Test // DATACASS-335
	void executeShouldForwardStatementToSession() {

		reactiveSession.execute("SELECT *").subscribe();

		verify(sessionMock).executeAsync(eq(SimpleStatement.newInstance("SELECT *")));
	}

	@Test // DATACASS-335
	void executeWithValuesShouldForwardStatementToSession() {

		reactiveSession.execute("SELECT * WHERE a = ? and b = ?", "A", "B").subscribe();

		verify(sessionMock).executeAsync(eq(SimpleStatement.newInstance("SELECT * WHERE a = ? and b = ?", "A", "B")));
	}

	@Test // DATACASS-335
	void executeWithValueMapShouldForwardStatementToSession() {

		reactiveSession.execute("SELECT * WHERE a = ?", Collections.singletonMap("a", "value")).subscribe();

		verify(sessionMock)
				.executeAsync(eq(SimpleStatement.newInstance("SELECT * WHERE a = ?", Collections.singletonMap("a", "value"))));
	}

	@Test // DATACASS-335
	void testPrepareQuery() {

		when(sessionMock.prepareAsync(any(SimpleStatement.class))).thenReturn(preparedStatementFuture);

		reactiveSession.prepare("SELECT *").subscribe();

		verify(sessionMock).prepareAsync(eq(SimpleStatement.newInstance("SELECT *")));
	}

	@Test // DATACASS-335
	void testPrepareStatement() {

		when(sessionMock.prepareAsync(any(SimpleStatement.class))).thenReturn(preparedStatementFuture);

		SimpleStatement statement = SimpleStatement.newInstance("SELECT *");
		reactiveSession.prepare(statement).subscribe();

		verify(sessionMock).prepareAsync(statement);
	}

	@Test // DATACASS-335
	void testClose() {

		reactiveSession.close();

		verify(sessionMock).close();
	}

	@Test // DATACASS-335
	void testIsClosed() {

		when(reactiveSession.isClosed()).thenReturn(true);

		boolean result = reactiveSession.isClosed();

		assertThat(result).isTrue();
		verify(sessionMock).isClosed();
	}

	@Test // DATACASS-509
	void shouldNotReadMoreThanAvailable() {

		AsyncResultSet resultSet = mock(AsyncResultSet.class);

		when(resultSet.remaining()).thenReturn(10);
		when(resultSet.currentPage())
				.thenReturn(IntStream.range(0, 10).mapToObj(value -> mock(Row.class)).collect(Collectors.toList()));

		future.complete(resultSet);
		when(resultSet.hasMorePages()).thenReturn(false);

		reactiveSession.execute(SimpleStatement.newInstance("")).flatMapMany(ReactiveResultSet::rows).collectList()
				.subscribe();

		verify(resultSet, never()).fetchNextPage();
	}

	@Test // DATACASS-529
	void shouldReadAvailableResults() {

		AsyncResultSet resultSet = mock(AsyncResultSet.class);
		when(resultSet.remaining()).thenReturn(10);
		when(resultSet.currentPage())
				.thenReturn(IntStream.range(0, 10).mapToObj(value -> mock(Row.class)).collect(Collectors.toList()));
		future.complete(resultSet);

		Flux<Row> flux = reactiveSession.execute(SimpleStatement.newInstance(""))
				.flatMapMany(ReactiveResultSet::availableRows);

		flux.as(StepVerifier::create).expectNextCount(10).verifyComplete();

		verify(resultSet, never()).fetchNextPage();
	}

	@Test // DATACASS-509
	void shouldFetchMore() {

		Iterator<Row> rows = mockIterator();

		AsyncResultSet resultSet = mock(AsyncResultSet.class);

		when(resultSet.remaining()).thenReturn(10);
		when(resultSet.currentPage())
				.thenReturn(IntStream.range(0, 10).mapToObj(value -> mock(Row.class)).collect(Collectors.toList()));

		AsyncResultSet emptyResultSet = mock(AsyncResultSet.class);

		when(emptyResultSet.currentPage()).thenReturn(Collections.emptyList());
		when(emptyResultSet.hasMorePages()).thenReturn(false);

		future.complete(resultSet);
		when(resultSet.hasMorePages()).thenReturn(true, false);
		when(resultSet.fetchNextPage()).thenReturn(new AsyncResult<>(emptyResultSet).completable());

		Flux<Row> flux = reactiveSession.execute(SimpleStatement.newInstance("")).flatMapMany(ReactiveResultSet::rows);

		StepVerifier.create(flux, 0).thenRequest(10).expectNextCount(10).then(() -> {
			verify(resultSet).fetchNextPage();
		}).thenRequest(10).verifyComplete();

		verify(emptyResultSet).hasMorePages();
		verify(emptyResultSet).currentPage();
		verifyNoMoreInteractions(emptyResultSet);
	}

	@SuppressWarnings("unchecked")
	private static Iterator<Row> mockIterator() {

		Row row = mock(Row.class);

		Iterator<Row> rows = mock(Iterator.class);

		when(rows.hasNext()).thenReturn(true);
		when(rows.next()).thenReturn(row);

		return rows;
	}

	@SuppressWarnings("all")
	private static <T extends Statement> T eq(T value) {

		return ArgumentMatchers
				.argThat(argument -> argument instanceof Statement ? value.toString().equals(argument.toString())
						: value.equals(argument));
	}
}
