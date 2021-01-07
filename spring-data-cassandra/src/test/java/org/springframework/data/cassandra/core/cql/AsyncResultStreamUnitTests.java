/*
 * Copyright 2019-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.springframework.util.concurrent.ListenableFuture;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Unit tests for {@link AsyncResultStream}.
 *
 * @author Mark Paluch
 */
class AsyncResultStreamUnitTests {

	private AsyncResultSet first = mock(AsyncResultSet.class);
	private AsyncResultSet last = mock(AsyncResultSet.class);
	private Row row1 = mock(Row.class);
	private Row row2 = mock(Row.class);

	@Test // DATACASS-656
	void shouldIterateFirstPage() {

		when(first.currentPage()).thenReturn(Collections.singletonList(row1));

		List<Row> rows = new ArrayList<>();

		AsyncResultStream.from(first).forEach(rows::add);

		assertThat(rows).containsOnly(row1);
	}

	@Test // DATACASS-656
	void shouldIterateMappedFirstPage() {

		when(first.currentPage()).thenReturn(Collections.singletonList(row1));

		List<String> rows = new ArrayList<>();

		AsyncResultStream.from(first).map((row, rowNum) -> "row-" + rowNum).forEach(rows::add);

		assertThat(rows).containsOnly("row-1");
	}

	@Test // DATACASS-656
	void shouldIterateMappedPages() {

		when(first.currentPage()).thenReturn(Collections.singletonList(row1));
		when(last.currentPage()).thenReturn(Collections.singletonList(row2));
		when(first.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(last));
		when(first.hasMorePages()).thenReturn(true);

		List<String> rows = new ArrayList<>();

		AsyncResultStream.from(first).map((row, rowNum) -> "row-" + rowNum).forEach(rows::add);

		assertThat(rows).containsOnly("row-1", "row-2");
	}

	@Test // DATACASS-656
	void shouldPropagateExceptionOnIterate() {

		when(first.currentPage()).thenReturn(Collections.singletonList(row1));

		CompletableFuture<AsyncResultSet> failed = new CompletableFuture<>();
		failed.completeExceptionally(new RuntimeException("boo"));
		when(first.fetchNextPage()).thenReturn(failed);
		when(first.hasMorePages()).thenReturn(true);

		List<String> rows = new ArrayList<>();

		ListenableFuture<Void> completion = AsyncResultStream.from(first).map((row, rowNum) -> "row-" + rowNum)
				.forEach(rows::add);

		assertThatThrownBy(completion::get).hasRootCauseInstanceOf(RuntimeException.class);
	}

	@Test // DATACASS-656
	void shouldCollectFirstPage() throws ExecutionException, InterruptedException {

		when(first.currentPage()).thenReturn(Collections.singletonList(row1));

		ListenableFuture<List<Row>> collect = AsyncResultStream.from(first).collect(Collectors.toList());

		assertThat(collect.get()).containsOnly(row1);
	}

	@Test // DATACASS-656
	void shouldCollectMappedPages() throws ExecutionException, InterruptedException {

		when(first.currentPage()).thenReturn(Collections.singletonList(row1));
		when(last.currentPage()).thenReturn(Collections.singletonList(row2));
		when(first.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(last));
		when(first.hasMorePages()).thenReturn(true);

		ListenableFuture<List<String>> rows = AsyncResultStream.from(first).map((row, rowNum) -> "row-" + rowNum)
				.collect(Collectors.toList());

		assertThat(rows.get()).containsOnly("row-1", "row-2");
	}

	@Test // DATACASS-656
	void shouldPropagateExceptionOnCollect() {

		when(first.currentPage()).thenReturn(Collections.singletonList(row1));

		CompletableFuture<AsyncResultSet> failed = new CompletableFuture<>();
		failed.completeExceptionally(new RuntimeException("boo"));
		when(first.fetchNextPage()).thenReturn(failed);
		when(first.hasMorePages()).thenReturn(true);

		ListenableFuture<List<Row>> collect = AsyncResultStream.from(first).collect(Collectors.toList());

		assertThatThrownBy(collect::get).hasRootCauseInstanceOf(RuntimeException.class);
	}
}
