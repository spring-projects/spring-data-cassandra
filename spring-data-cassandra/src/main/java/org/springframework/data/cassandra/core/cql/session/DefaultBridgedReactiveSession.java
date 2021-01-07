/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Default implementation of a {@link ReactiveSession}. This implementation bridges asynchronous {@link CqlSession}
 * methods to reactive execution patterns.
 * <p>
 * Calls are deferred until a subscriber subscribes to the resulting {@link org.reactivestreams.Publisher}. The calls
 * are executed by subscribing to {@link CompletionStage} and returning the result as calls complete.
 * <p>
 * Elements are emitted on netty EventLoop threads. {@link AsyncResultSet} allows {@link AsyncResultSet#fetchNextPage()}
 * asynchronous requesting} of subsequent pages. The next page is requested after emitting all elements of the previous
 * page. However, this is an intermediate solution until Datastax can provide a fully reactive driver.
 * <p>
 * All CQL operations performed by this class are logged at debug level, using
 * {@code org.springframework.data.cassandra.core.cql.DefaultBridgedReactiveSession} as log category.
 * <p>
 *
 * @author Mark Paluch
 * @author Mateusz Stefek
 * @since 2.0
 * @see Mono
 * @see ReactiveResultSet
 * @see Scheduler
 * @see ReactiveSession
 */
public class DefaultBridgedReactiveSession implements ReactiveSession {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final CqlSession session;

	/**
	 * Create a new {@link DefaultBridgedReactiveSession} for a {@link CqlSession}.
	 *
	 * @param session must not be {@literal null}.
	 * @since 2.1
	 */
	public DefaultBridgedReactiveSession(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		this.session = session;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return this.session.isClosed();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#getContext()
	 */
	@Override
	public DriverContext getContext() {
		return this.session.getContext();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#execute(java.lang.String)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query) {

		Assert.hasText(query, "Query must not be empty");

		return execute(SimpleStatement.newInstance(query));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#execute(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query, Object... values) {

		Assert.hasText(query, "Query must not be empty");

		return execute(SimpleStatement.newInstance(query, values));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#execute(java.lang.String, java.util.Map)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(String query, Map<String, Object> values) {

		Assert.hasText(query, "Query must not be empty");

		return execute(SimpleStatement.newInstance(query, values));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#execute(com.datastax.oss.driver.api.core.cql.Statement)
	 */
	@Override
	public Mono<ReactiveResultSet> execute(Statement<?> statement) {

		Assert.notNull(statement, "Statement must not be null");

		return Mono.fromCompletionStage(() -> {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing statement [{}]", getCql(statement));
			}

			return this.session.executeAsync(statement);
		}).map(DefaultReactiveResultSet::new);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#prepare(java.lang.String)
	 */
	@Override
	public Mono<PreparedStatement> prepare(String query) {

		Assert.hasText(query, "Query must not be empty");

		return prepare(SimpleStatement.newInstance(query));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#prepare(com.datastax.oss.driver.api.core.cql.SimpleStatement)
	 */
	@Override
	public Mono<PreparedStatement> prepare(SimpleStatement statement) {

		Assert.notNull(statement, "Statement must not be null");

		return Mono.fromCompletionStage(() -> {

			if (logger.isDebugEnabled()) {
				logger.debug("Preparing statement [{}]", getCql(statement));
			}

			return this.session.prepareAsync(statement);
		});
	}

	private static String getCql(Object statement) {

		if (statement instanceof SimpleStatement) {
			return ((SimpleStatement) statement).getQuery();
		}

		if (statement instanceof PreparedStatement) {
			return ((PreparedStatement) statement).getQuery();
		}

		if (statement instanceof BoundStatement) {
			return getCql(((BoundStatement) statement).getPreparedStatement());
		}

		if (statement instanceof BatchStatement) {

			StringBuilder builder = new StringBuilder();

			for (BatchableStatement<?> batchableStatement : ((BatchStatement) statement)) {

				String query = getCql(batchableStatement);
				builder.append(query).append(query.endsWith(";") ? "" : ";");
			}

			return builder.toString();
		}

		return String.format("Unknown: %s", statement);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.ReactiveSession#close()
	 */
	@Override
	public void close() {
		this.session.close();
	}

	static class DefaultReactiveResultSet implements ReactiveResultSet {

		private final AsyncResultSet resultSet;
		private final boolean wasApplied;

		DefaultReactiveResultSet(AsyncResultSet resultSet) {
			this.resultSet = resultSet;

			boolean wasApplied;
			try {
				wasApplied = resultSet.wasApplied();
			} catch (Exception e) {
				wasApplied = false;
			}

			this.wasApplied = wasApplied;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.ReactiveResultSet#rows()
		 */
		@Override
		public Flux<Row> rows() {
			return getRows(Mono.just(this.resultSet));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.ReactiveResultSet#availableRows()
		 */
		@Override
		public Flux<Row> availableRows() {
			return toRows(this.resultSet);
		}

		private Flux<Row> getRows(Mono<AsyncResultSet> nextResults) {

			return nextResults.flatMapMany(it -> {

				Flux<Row> rows = toRows(it);

				if (!it.hasMorePages()) {
					return rows;
				}

				MonoProcessor<AsyncResultSet> processor = MonoProcessor.create();

				return rows.doOnComplete(() -> fetchMore(it.fetchNextPage(), processor)).concatWith(getRows(processor));
			});
		}

		static Flux<Row> toRows(AsyncResultSet resultSet) {
			return Flux.fromIterable(resultSet.currentPage());
		}

		static void fetchMore(CompletionStage<AsyncResultSet> future, MonoProcessor<AsyncResultSet> sink) {

			try {

				future.whenComplete((rs, err) -> {

					if (err != null) {
						sink.onError(err);
					} else {
						sink.onNext(rs);
						sink.onComplete();
					}
				});

			} catch (Exception cause) {
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
			return this.wasApplied;
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
			return Collections.singletonList(getExecutionInfo());
		}
	}

}
