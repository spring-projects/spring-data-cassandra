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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Default implementation of a {@link ReactiveSession}. This implementation bridges asynchronous {@link Session} methods
 * to reactive execution patterns.
 * <p>
 * Calls are deferred until a subscriber subscribes to the resulting {@link org.reactivestreams.Publisher}. The calls
 * are executed by subscribing to {@link ListenableFuture} and returning the result as calls complete.
 * <p>
 * {@link ResultSet} implements transparent paging that invokes in the middle of result streaming blocking calls to
 * Cassandra. {@link DefaultBridgedReactiveSession} uses therefore {@link ReactiveResultSet} to avoid client thread
 * blocking. Elements are emitted on netty EventLoop threads and transported by the provided {@link Scheduler}. However,
 * this is an intermediate solution until Datastax can provide a fully reactive driver.
 * <p>
 * All CQL operations performed by this class are logged at debug level, using
 * "org.springframework.cassandra.core.DefaultBridgedReactiveSession" as log category.
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
	private final Scheduler scheduler;

	/**
	 * Creates a new {@link DefaultBridgedReactiveSession} for a {@link Session} and {@link Scheduler}.
	 * 
	 * @param session must not be {@literal null}.
	 * @param scheduler must not be {@literal null}.
	 */
	public DefaultBridgedReactiveSession(Session session, Scheduler scheduler) {

		Assert.notNull(session, "Session must not be null");
		Assert.notNull(scheduler, "Scheduler must not be null");

		this.session = session;
		this.scheduler = scheduler;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#execute(java.lang.String)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query) {

		Assert.hasText(query, "Query must not be empty");

		return execute(new SimpleStatement(query));
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#execute(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query, Object... values) {

		Assert.hasText(query, "Query must not be empty");

		return execute(new SimpleStatement(query, values));
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#execute(java.lang.String, java.util.Map)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query, Map<String, Object> values) {

		Assert.hasText(query, "Query must not be empty");

		return execute(new SimpleStatement(query, values));
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#execute(com.datastax.driver.core.Statement)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(Statement statement) {

		Assert.notNull(statement, "Statement must not be null");

		return Mono.defer(() -> {

			try {

				if (logger.isDebugEnabled()) {
					logger.debug("Executing Statement [{}]", statement);
				}

				CompletableFuture<ReactiveResultSet> future = new CompletableFuture<>();
				ResultSetFuture resultSetFuture = session.executeAsync(statement);

				resultSetFuture.addListener(() -> {

					if (resultSetFuture.isDone()) {

						try {
							future.complete(new DefaultReactiveResultSet(resultSetFuture.getUninterruptibly(), scheduler));
						} catch (Exception e) {
							future.completeExceptionally(e);
						}
					}
				}, Runnable::run);

				return Mono.fromFuture(future);
			} catch (Exception e) {
				return Mono.error(e);
			}

		}).subscribeOn(scheduler);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#prepare(java.lang.String)
	 */
	@Override
	public Mono<PreparedStatement> prepare(String query) {

		Assert.hasText(query, "Query must not be empty");

		return prepare(new SimpleStatement(query));
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#prepare(com.datastax.driver.core.RegularStatement)
	 */
	@Override
	public Mono<PreparedStatement> prepare(RegularStatement statement) {

		Assert.notNull(statement, "Statement must not be null");

		return Mono.defer(() -> {

			try {

				if (logger.isDebugEnabled()) {
					logger.debug("Preparing Statement [{}]", statement);
				}

				CompletableFuture<PreparedStatement> future = new CompletableFuture<>();
				ListenableFuture<PreparedStatement> resultSetFuture = session.prepareAsync(statement);

				resultSetFuture.addListener(() -> {

					if (resultSetFuture.isDone()) {
						try {
							future.complete(resultSetFuture.get());
						} catch (Exception e) {
							future.completeExceptionally(e);
						}
					}
				}, Runnable::run);

				return Mono.fromFuture(future);
			} catch (Exception e) {
				return Mono.error(e);
			}

		}).subscribeOn(scheduler);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#close()
	 */
	@Override
	public void close() {
		session.close();
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return session.isClosed();
	}

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.ReactiveSession#getCluster()
	 */
	@Override
	public Cluster getCluster() {
		return session.getCluster();
	}

	private static class DefaultReactiveResultSet implements ReactiveResultSet {

		private final ResultSet resultSet;
		private final Scheduler scheduler;

		DefaultReactiveResultSet(ResultSet resultSet, Scheduler scheduler) {
			this.resultSet = resultSet;
			this.scheduler = scheduler;
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.ReactiveResultSet#rows()
		 */
		@Override
		public Flux<Row> rows() {

			int prefetch = Math.max(1, resultSet.getAvailableWithoutFetching());
			return Flux.fromIterable(resultSet) //
					.subscribeOn(scheduler) //
					.publishOn(Schedulers.immediate(), prefetch); // limit prefetching to available size
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.ReactiveResultSet#getColumnDefinitions()
		 */
		@Override
		public ColumnDefinitions getColumnDefinitions() {
			return resultSet.getColumnDefinitions();
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.ReactiveResultSet#wasApplied()
		 */
		@Override
		public boolean wasApplied() {
			return resultSet.wasApplied();
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.ReactiveResultSet#getExecutionInfo()
		 */
		@Override
		public ExecutionInfo getExecutionInfo() {
			return resultSet.getExecutionInfo();
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.ReactiveResultSet#getAllExecutionInfo()
		 */
		@Override
		public List<ExecutionInfo> getAllExecutionInfo() {
			return resultSet.getAllExecutionInfo();
		}
	}
}
