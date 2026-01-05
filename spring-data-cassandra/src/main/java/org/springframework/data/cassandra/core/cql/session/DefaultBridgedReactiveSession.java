/*
 * Copyright 2017-present the original author or authors.
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
import reactor.core.scheduler.Scheduler;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
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
import com.datastax.oss.driver.api.core.metadata.Metadata;

/**
 * Default implementation of a {@link ReactiveSession}. This implementation bridges asynchronous {@link CqlSession}
 * methods to reactive execution patterns.
 * <p>
 * Calls are deferred until a subscriber subscribes to the resulting {@link org.reactivestreams.Publisher}. The calls
 * are executed by subscribing to {@link CompletionStage} and returning the result as calls complete.
 * <p>
 * Elements are emitted on netty EventLoop threads. {@link AsyncResultSet} allows {@link AsyncResultSet#fetchNextPage()
 * asynchronous requesting} of subsequent pages. The next page is requested after emitting all elements of the previous
 * page. However, this is an intermediate solution until Datastax can provide a fully reactive driver.
 * <p>
 * All CQL operations performed by this class are logged at debug level, using
 * {@code org.springframework.data.cassandra.core.cql.DefaultBridgedReactiveSession} as log category.
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

	private final Log log = LogFactory.getLog(getClass());

	private final CqlSession session;

	/**
	 * Create a new {@link DefaultBridgedReactiveSession} for a {@link CqlSession}.
	 *
	 * @param session must not be {@literal null}.
	 * @since 2.1
	 */
	public DefaultBridgedReactiveSession(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		// potentially unwrap a ObservationDecoratedProxy as reactive observability
		// requires its own approach to span creation. We do not want to participate in
		// async API spans but rather drive our own spans.
		if (session instanceof TargetSource) {
			Class<?>[] interfaces = session.getClass().getInterfaces();
			for (Class<?> anInterface : interfaces) {
				if (anInterface.getName().endsWith("ObservationDecoratedProxy")) {
					session = (CqlSession) AopProxyUtils.getSingletonTarget(session);
				}
			}
		}

		this.session = session;
	}

	@Override
	public Metadata getMetadata() {
		return this.session.getMetadata();
	}

	@Override
	public Optional<CqlIdentifier> getKeyspace() {
		return this.session.getKeyspace();
	}

	@Override
	public boolean isClosed() {
		return this.session.isClosed();
	}

	@Override
	public DriverContext getContext() {
		return this.session.getContext();
	}

	@Override
	public Mono<ReactiveResultSet> execute(String query) {

		Assert.hasText(query, "Query must not be empty");

		return execute(SimpleStatement.newInstance(query));
	}

	@Override
	public Mono<ReactiveResultSet> execute(String query, Object... values) {

		Assert.hasText(query, "Query must not be empty");

		return execute(SimpleStatement.newInstance(query, values));
	}

	@Override
	public Mono<ReactiveResultSet> execute(String query, Map<String, Object> values) {

		Assert.hasText(query, "Query must not be empty");

		return execute(SimpleStatement.newInstance(query, values));
	}

	@Override
	public Mono<ReactiveResultSet> execute(Statement<?> statement) {

		Assert.notNull(statement, "Statement must not be null");

		return Mono.fromCompletionStage(() -> {

			if (log.isDebugEnabled()) {
				log.debug(String.format("Executing statement [%s]", getCql(statement)));
			}

			return this.session.executeAsync(statement);
		}).map(DefaultReactiveResultSet::new);
	}

	@Override
	public Mono<PreparedStatement> prepare(String query) {

		Assert.hasText(query, "Query must not be empty");

		return prepare(SimpleStatement.newInstance(query));
	}

	@Override
	public Mono<PreparedStatement> prepare(SimpleStatement statement) {

		Assert.notNull(statement, "Statement must not be null");

		return Mono.fromCompletionStage(() -> {

			if (log.isDebugEnabled()) {
				log.debug(String.format("Preparing statement [%s]", getCql(statement)));
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

		@Override
		public Flux<Row> rows() {

			return Mono.just(this.resultSet).expand(asyncResultSet -> {
				if (asyncResultSet.hasMorePages()) {
					return Mono.fromCompletionStage(asyncResultSet.fetchNextPage());
				}
				return Mono.empty();
			}).flatMapIterable(AsyncPagingIterable::currentPage);
		}

		@Override
		public Flux<Row> availableRows() {
			return Flux.fromIterable(resultSet.currentPage());
		}

		@Override
		public ColumnDefinitions getColumnDefinitions() {
			return this.resultSet.getColumnDefinitions();
		}

		@Override
		public boolean wasApplied() {
			return this.wasApplied;
		}

		@Override
		public ExecutionInfo getExecutionInfo() {
			return this.resultSet.getExecutionInfo();
		}

		@Override
		public List<ExecutionInfo> getAllExecutionInfo() {
			return Collections.singletonList(getExecutionInfo());
		}
	}

}
