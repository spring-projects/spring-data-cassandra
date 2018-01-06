/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.util.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Default implementation of a {@link ReactiveSession}. This implementation bridges asynchronous {@link Session} methods
 * to reactive execution patterns.
 * <p>
 * Calls are deferred until a subscriber subscribes to the resulting {@link org.reactivestreams.Publisher}. The calls
 * are executed by subscribing to {@link ListenableFuture} and returning the result as calls complete.
 * <p>
 * Elements are emitted on netty EventLoop threads. {@link ResultSet} allows {@link ResultSet#fetchMoreResults()
 * asynchronous requesting} of subsequent pages. The next page is requested after emitting all elements of the previous
 * page. However, this is an intermediate solution until Datastax can provide a fully reactive driver.
 * <p>
 * All CQL operations performed by this class are logged at debug level, using
 * {@code org.springframework.data.cassandra.core.cql.DefaultBridgedReactiveSession} as log category.
 * <p>
 *
 * @author Mark Paluch
 * @since 2.0
 * @see Mono
 * @see ReactiveResultSet
 * @see Scheduler
 * @see ReactiveSession
 */
public class DefaultBridgedReactiveSession implements ReactiveSession {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final Session session;

	/**
	 * Create a new {@link DefaultBridgedReactiveSession} for a {@link Session}.
	 *
	 * @param session must not be {@literal null}.
	 * @since 2.1
	 */
	public DefaultBridgedReactiveSession(Session session) {

		Assert.notNull(session, "Session must not be null");

		this.session = session;
	}

	/**
	 * Create a new {@link DefaultBridgedReactiveSession} for a {@link Session} and {@link Scheduler}.
	 *
	 * @param session must not be {@literal null}.
	 * @param scheduler must not be {@literal null}.
	 * @deprecated since 2.1. Use {@link #DefaultBridgedReactiveSession(Session)} as a {@link Scheduler} is no longer
	 *             required to off-load {@link ResultSet}'s blocking behavior.
	 */
	@Deprecated
	public DefaultBridgedReactiveSession(Session session, Scheduler scheduler) {
		this(session);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return this.session.isClosed();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#getCluster()
	 */
	@Override
	public Cluster getCluster() {
		return this.session.getCluster();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#execute(java.lang.String)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query) {

		Assert.hasText(query, "Query must not be empty");

		return execute(new SimpleStatement(query));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#execute(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query, Object... values) {

		Assert.hasText(query, "Query must not be empty");

		return execute(new SimpleStatement(query, values));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#execute(java.lang.String, java.util.Map)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query, Map<String, Object> values) {

		Assert.hasText(query, "Query must not be empty");

		return execute(new SimpleStatement(query, values));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#execute(com.datastax.driver.core.Statement)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(Statement statement) {

		Assert.notNull(statement, "Statement must not be null");

		return Mono.create(sink -> {

			try {
				if (logger.isDebugEnabled()) {
					logger.debug("Executing Statement [{}]", statement);
				}

				ListenableFuture<ResultSet> future = this.session.executeAsync(statement);

				ListenableFuture<ReactiveResultSet> resultSetFuture =
					Futures.transform(future, DefaultReactiveResultSet::new);

				adaptFuture(resultSetFuture, sink);
			}
			catch (Exception cause) {
				sink.error(cause);
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#prepare(java.lang.String)
	 */
	@Override
	public Mono<PreparedStatement> prepare(String query) {

		Assert.hasText(query, "Query must not be empty");

		return prepare(new SimpleStatement(query));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#prepare(com.datastax.driver.core.RegularStatement)
	 */
	@Override
	public Mono<PreparedStatement> prepare(RegularStatement statement) {

		Assert.notNull(statement, "Statement must not be null");

		return Mono.create(sink -> {

			try {
				if (logger.isDebugEnabled()) {
					logger.debug("Preparing Statement [{}]", statement);
				}

				ListenableFuture<PreparedStatement> resultSetFuture = this.session.prepareAsync(statement);

				adaptFuture(resultSetFuture, sink);
			}
			catch (Exception cause) {
				sink.error(cause);
			}
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#close()
	 */
	@Override
	public void close() {
		this.session.close();
	}

	/**
	 * Adapt {@link ListenableFuture} signals (completion, error) and propagate these to {@link MonoSink}.
	 *
	 * @param future the originating future.
	 * @param sink the sink receiving signals.
	 */
	private static <T> void adaptFuture(ListenableFuture<T> future, MonoSink<T> sink) {

		future.addListener(() -> {

			if (future.isDone()) {
				try {
					sink.success(future.get());
				}
				catch (ExecutionException cause) {
					sink.error(cause.getCause());
				}
				catch (Exception cause) {
					sink.error(cause);
				}
			}
		}, Runnable::run);
	}

	static class DefaultReactiveResultSet implements ReactiveResultSet {

		private final ResultSet resultSet;

		DefaultReactiveResultSet(ResultSet resultSet) {
			this.resultSet = resultSet;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.ReactiveResultSet#rows()
		 */
		@Override
		public Flux<Row> rows() {
			return getRows(Mono.just(this.resultSet));
		}

		Flux<Row> getRows(Mono<ResultSet> nextResults) {

			return nextResults.flatMapMany(it -> {

				Flux<Row> rows = toRows(it);

				if (it.isFullyFetched()) {
					return rows;
				}

				MonoProcessor<ResultSet> processor = MonoProcessor.create();

				return rows
					.doOnComplete(() -> fetchMore(it.fetchMoreResults(), processor))
					.concatWith(getRows(processor));
			});
		}

		static Flux<Row> toRows(ResultSet resultSet) {

			int prefetch = Math.max(1, resultSet.getAvailableWithoutFetching());

			return Flux.fromIterable(resultSet).take(prefetch);
		}

		static void fetchMore(ListenableFuture<ResultSet> future, MonoProcessor<ResultSet> sink) {

			try {

				future.addListener(() -> {

					try {
						sink.onNext(future.get());
						sink.onComplete();
					}
					catch (ExecutionException cause) {
						sink.onError(cause.getCause());
					}
					catch (Exception cause) {
						sink.onError(cause);
					}
				}, Runnable::run);

			}
			catch (Exception cause) {
				sink.onError(cause);
			}
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.ReactiveResultSet#getColumnDefinitions()
		 */
		@Override
		public ColumnDefinitions getColumnDefinitions() {
			return this.resultSet.getColumnDefinitions();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.ReactiveResultSet#wasApplied()
		 */
		@Override
		public boolean wasApplied() {
			return this.resultSet.wasApplied();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.ReactiveResultSet#getExecutionInfo()
		 */
		@Override
		public ExecutionInfo getExecutionInfo() {
			return this.resultSet.getExecutionInfo();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.ReactiveResultSet#getAllExecutionInfo()
		 */
		@Override
		public List<ExecutionInfo> getAllExecutionInfo() {
			return this.resultSet.getAllExecutionInfo();
		}
	}
}
