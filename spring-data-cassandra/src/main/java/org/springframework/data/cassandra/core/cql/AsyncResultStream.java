/*
 * Copyright 2019-2020 the original author or authors.
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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collector;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * Asynchronous supplied sequence of elements supporting sequential operations over a {@link AsyncResultSet a result
 * set}. An asynchronous stream represents a pipeline of operations to process a {@link AsyncResultSet}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
class AsyncResultStream<T> {

	private final AsyncResultSet resultSet;

	private final RowMapper<T> mapper;

	private AsyncResultStream(AsyncResultSet resultSet, RowMapper<T> mapper) {
		this.resultSet = resultSet;
		this.mapper = mapper;
	}

	/**
	 * Creates a {@link AsyncResultStream} given {@link AsyncResultSet}.
	 *
	 * @param resultSet the result set to process.
	 * @return a new {@link AsyncResultStream} instance.
	 */
	static AsyncResultStream<Row> from(AsyncResultSet resultSet) {

		Assert.notNull(resultSet, "AsyncResultSet must not be null");

		return new AsyncResultStream<>(resultSet, (row, rowNum) -> row);
	}

	/**
	 * Returns a stream consisting of the results of applying the given function to the elements of this stream.
	 * <p>
	 * This is an intermediate operation.
	 *
	 * @param <R> The element type of the new stream
	 * @param mapper a non-interfering, stateless {@link RowMapper}.
	 */
	<R> AsyncResultStream<R> map(RowMapper<R> mapper) {

		Assert.notNull(mapper, "RowMapper must not be null");

		return new AsyncResultStream<>(resultSet, mapper);
	}

	/**
	 * Performs a mutable reduction operation on the elements of this stream using a {@link Collector} resulting in a
	 * {@link ListenableFuture}.
	 * <p>
	 * This is a terminal operation.
	 *
	 * @param <R> the type of the result
	 * @param <A> the intermediate accumulation type of the {@link Collector}
	 * @param collector the {@link Collector} describing the reduction
	 * @return the result of the reduction
	 */
	<R, A> ListenableFuture<R> collect(Collector<? super T, A, R> collector) {

		Assert.notNull(collector, "Collector must not be null");

		SettableListenableFuture<R> future = new SettableListenableFuture<>();
		CollectState<A, R> collectState = new CollectState<>(collector);

		collectState.collectAsync(future, this.resultSet);

		return future;
	}

	/**
	 * Performs an action for each element of this stream. This method returns a {@link ListenableFuture} that completes
	 * without a value ({@code null}) once all elements have been processed.
	 * <p>
	 * This is a terminal operation.
	 * <p>
	 * If the action accesses shared state, it is responsible for providing the required synchronization.
	 *
	 * @param action a non-interfering action to perform on the elements.
	 */
	ListenableFuture<Void> forEach(Consumer<T> action) {

		Assert.notNull(action, "Action must not be null");

		SettableListenableFuture<Void> future = new SettableListenableFuture<>();
		ForwardLoopState loopState = new ForwardLoopState(action);

		loopState.forEachAsync(future, this.resultSet);

		return future;
	}

	/**
	 * State object for forward-looping using {@code forEach}.
	 */
	class ForwardLoopState {

		private final AtomicInteger rowNumber = new AtomicInteger();
		private final Consumer<T> consumer;

		ForwardLoopState(Consumer<T> consumer) {
			this.consumer = consumer;
		}

		void peekRow(Iterable<Row> rows) {
			rows.forEach(row -> consumer.accept(mapper.mapRow(row, rowNumber.incrementAndGet())));
		}

		/**
		 * Recursive async iteration.
		 *
		 * @param target
		 * @param resultSet
		 */
		void forEachAsync(SettableListenableFuture<Void> target, AsyncResultSet resultSet) {

			if (target.isCancelled()) {
				return;
			}

			try {
				peekRow(resultSet.currentPage());
			} catch (RuntimeException e) {
				target.setException(e);
				return;
			}

			if (!resultSet.hasMorePages()) {
				target.set(null);
			} else {

				CompletionStage<AsyncResultSet> nextPage = resultSet.fetchNextPage();

				nextPage.whenComplete((nextResultSet, throwable) -> {

					if (throwable != null) {
						target.setException(throwable);
					} else {
						forEachAsync(target, nextResultSet);
					}
				});
			}
		}
	}

	/**
	 * State object for collecting rows using {@code collect}.
	 */
	class CollectState<A, R> {

		private final AtomicInteger rowNumber = new AtomicInteger();
		private volatile A intermediate;
		private final Collector<? super T, A, R> collector;

		CollectState(Collector<? super T, A, R> collector) {
			this.collector = collector;
			this.intermediate = collector.supplier().get();
		}

		void collectPage(Iterable<Row> rows) {

			for (Row row : rows) {
				collector.accumulator().accept(intermediate, mapper.mapRow(row, rowNumber.incrementAndGet()));
			}
		}

		R finish() {
			return collector.finisher().apply(intermediate);
		}

		/**
		 * Recursive collection.
		 *
		 * @param target
		 * @param resultSet
		 */
		void collectAsync(SettableListenableFuture<R> target, AsyncResultSet resultSet) {

			if (target.isCancelled()) {
				return;
			}

			try {
				collectPage(resultSet.currentPage());
			} catch (RuntimeException e) {
				target.setException(e);
				return;
			}

			if (!resultSet.hasMorePages()) {
				target.set(finish());
			} else {

				CompletionStage<AsyncResultSet> nextPage = resultSet.fetchNextPage();

				nextPage.whenComplete((nextResultSet, throwable) -> {

					if (throwable != null) {
						target.setException(throwable);
					} else {
						collectAsync(target, nextResultSet);
					}
				});
			}
		}
	}
}
