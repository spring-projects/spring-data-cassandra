/*
 * Copyright 2016-2020 the original author or authors.
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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.Spliterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Unit tests for {@link DefaultBridgedReactiveSession}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultBridgedReactiveSessionUnitTests {

	@Mock Session sessionMock;
	@Mock Spliterator<Row> spliteratorMock;
	@Mock ResultSetFuture future;
	@Mock ListenableFuture<PreparedStatement> preparedStatementFuture;

	private DefaultBridgedReactiveSession reactiveSession;

	@Before
	public void before() {

		reactiveSession = new DefaultBridgedReactiveSession(sessionMock);

		when(sessionMock.executeAsync(any(Statement.class))).thenReturn(future);
		when(sessionMock.prepareAsync(any(RegularStatement.class))).thenReturn(preparedStatementFuture);

		doAnswer(invocation -> {

			Runnable listener = invocation.getArgument(0);

			listener.run();

			return null;
		}).when(future).addListener(any(), any());

		when(future.isDone()).thenReturn(true);
	}

	@Test // DATACASS-335
	public void executeStatementShouldForwardStatementToSession() {

		SimpleStatement statement = new SimpleStatement("SELECT *");

		reactiveSession.execute(statement).subscribe();

		verify(sessionMock).executeAsync(statement);
	}

	@Test // DATACASS-335
	public void executeShouldForwardStatementToSession() {

		reactiveSession.execute("SELECT *").subscribe();

		verify(sessionMock).executeAsync(eq(new SimpleStatement("SELECT *")));
	}

	@Test // DATACASS-335
	public void executeWithValuesShouldForwardStatementToSession() {

		reactiveSession.execute("SELECT * WHERE a = ? and b = ?", "A", "B").subscribe();

		verify(sessionMock).executeAsync(eq(new SimpleStatement("SELECT * WHERE a = ? and b = ?", "A", "B")));
	}

	@Test // DATACASS-335
	public void executeWithValueMapShouldForwardStatementToSession() {

		reactiveSession.execute("SELECT * WHERE a = ?", Collections.singletonMap("a", "value")).subscribe();

		verify(sessionMock)
				.executeAsync(eq(new SimpleStatement("SELECT * WHERE a = ?", Collections.singletonMap("a", "value"))));
	}

	@Test // DATACASS-335
	public void testPrepareQuery() {

		reactiveSession.prepare("SELECT *").subscribe();

		verify(sessionMock).prepareAsync(eq(new SimpleStatement("SELECT *")));
	}

	@Test // DATACASS-335
	public void testPrepareStatement() {

		SimpleStatement statement = new SimpleStatement("SELECT *");
		reactiveSession.prepare(statement).subscribe();

		verify(sessionMock).prepareAsync(statement);
	}

	@Test // DATACASS-335
	public void testClose() {

		reactiveSession.close();

		verify(sessionMock).close();
	}

	@Test // DATACASS-335
	public void testIsClosed() {

		when(reactiveSession.isClosed()).thenReturn(true);

		boolean result = reactiveSession.isClosed();

		assertThat(result).isTrue();
		verify(sessionMock).isClosed();
	}

	@Test // DATACASS-335
	public void testGetCluster() {

		Cluster clusterMock = mock(Cluster.class);
		when(sessionMock.getCluster()).thenReturn(clusterMock);

		Cluster result = reactiveSession.getCluster();

		assertThat(result).isSameAs(clusterMock);
	}

	@Test // DATACASS-509
	public void shouldNotReadMoreThanAvailable() throws Exception {

		Iterator<Row> rows = mockIterator();

		ResultSet resultSet = mock(ResultSet.class);

		when(resultSet.getAvailableWithoutFetching()).thenReturn(10);
		when(resultSet.iterator()).thenReturn(rows);
		when(resultSet.spliterator()).thenReturn(spliteratorMock);

		when(future.get()).thenReturn(resultSet);
		when(resultSet.isFullyFetched()).thenReturn(true);

		reactiveSession.execute(new SimpleStatement("")).flatMapMany(ReactiveResultSet::rows).collectList().subscribe();

		verify(rows, times(10)).next();
		verify(resultSet, never()).fetchMoreResults();
	}

	@Test // DATACASS-529
	public void shouldReadAvailableResults() throws Exception {

		Iterator<Row> rows = mockIterator();

		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.iterator()).thenReturn(rows);
		when(resultSet.getAvailableWithoutFetching()).thenReturn(10);
		when(resultSet.spliterator()).thenReturn(spliteratorMock);
		when(future.get()).thenReturn(resultSet);

		Flux<Row> flux = reactiveSession.execute(new SimpleStatement("")).flatMapMany(ReactiveResultSet::availableRows);

		flux.as(StepVerifier::create).expectNextCount(10).verifyComplete();

		verify(rows, times(10)).next();
		verify(future, times(1)).addListener(any(), any());
		verify(resultSet, never()).fetchMoreResults();
	}

	@Test // DATACASS-509
	public void shouldFetchMore() throws Exception {

		Iterator<Row> rows = mockIterator();

		ResultSet resultSet = mock(ResultSet.class);

		when(resultSet.getAvailableWithoutFetching()).thenReturn(10);
		when(resultSet.iterator()).thenReturn(rows);
		when(resultSet.spliterator()).thenReturn(spliteratorMock);

		ResultSet emptyResultSet = mock(ResultSet.class);

		when(emptyResultSet.iterator()).thenReturn(Collections.emptyIterator());
		when(emptyResultSet.isFullyFetched()).thenReturn(true);
		when(emptyResultSet.spliterator()).thenReturn(spliteratorMock);

		when(future.get()).thenReturn(resultSet);
		when(resultSet.isFullyFetched()).thenReturn(false, true);
		when(resultSet.fetchMoreResults()).thenReturn(Futures.immediateFuture(emptyResultSet));

		Flux<Row> flux = reactiveSession.execute(new SimpleStatement("")).flatMapMany(ReactiveResultSet::rows);

		StepVerifier.create(flux, 0).thenRequest(10).expectNextCount(10).then(() -> {

			verify(rows, times(10)).next();
			verify(resultSet).fetchMoreResults();
		}).thenRequest(10).verifyComplete();
	}

	@Test // DATACASS-509
	public void shouldFetchDependingOnCompletion() throws Exception {

		Iterator<Row> rows = mockIterator();

		Queue<Runnable> runnables = new ArrayDeque<>();

		ResultSet resultSet = mock(ResultSet.class);

		when(resultSet.getAvailableWithoutFetching()).thenReturn(10);
		when(resultSet.iterator()).thenReturn(rows);
		when(resultSet.spliterator()).thenReturn(spliteratorMock);

		reset(future);
		doAnswer(invocation -> {
			runnables.offer(invocation.getArgument(0));
			return null;
		}).when(future).addListener(any(), any());

		when(future.isDone()).thenReturn(true);
		when(future.get()).thenReturn(resultSet);
		when(resultSet.isFullyFetched()).thenReturn(false, false, true);
		when(resultSet.fetchMoreResults()).thenReturn(future);

		Flux<Row> flux = reactiveSession.execute(new SimpleStatement("")).flatMapMany(ReactiveResultSet::rows);

		StepVerifier.create(flux, 0) //
				.then(() -> runnables.poll().run()) // complete the first future from executeAsync()
				.thenRequest(9).expectNextCount(9).then(() -> {
					// feed the 9 elements from the initial ResultSet
					verify(resultSet, never()).fetchMoreResults();
				}).thenRequest(1).expectNextCount(1).then(() -> {

					// initial ResultSet exhausted, fetch next chunk
					verify(resultSet).fetchMoreResults();
					runnables.poll().run();
				}).thenRequest(1).expectNextCount(1).then(() -> {

					// first element from the second ResultSet received, no subsequent fetch
					assertThat(runnables).isEmpty();
				}).thenRequest(19).expectNextCount(9).then(() -> {

					// second ResultSet exhausted
					assertThat(runnables).hasSize(1);
					runnables.poll().run();
				}).thenRequest(10).expectNextCount(10).verifyComplete();
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
