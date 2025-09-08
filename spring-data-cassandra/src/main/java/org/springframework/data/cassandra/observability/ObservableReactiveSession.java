/*
 * Copyright 2022-2025 the original author or authors.
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
package org.springframework.data.cassandra.observability;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.ReactiveResultSet;
import org.springframework.data.cassandra.ReactiveSession;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;

/**
 * Instrumented {@link ReactiveSession} for observability.
 *
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @since 4.0
 */
public class ObservableReactiveSession implements ReactiveSession {

	private final ReactiveSession delegate;

	private final String remoteServiceName;

	private final ObservationRegistry observationRegistry;

	private final CassandraObservationConvention convention;

	ObservableReactiveSession(ReactiveSession delegate, String remoteServiceName,
			CassandraObservationConvention convention, ObservationRegistry observationRegistry) {
		this.delegate = delegate;
		this.remoteServiceName = remoteServiceName;
		this.convention = convention;
		this.observationRegistry = observationRegistry;
	}

	/**
	 * Factory method for creation of a {@link ObservableReactiveSession}.
	 *
	 * @param session reactive session.
	 * @param observationRegistry observation registry.
	 * @return traced representation of a {@link ReactiveSession}.
	 */
	public static ReactiveSession create(ReactiveSession session, ObservationRegistry observationRegistry) {
		return new ObservableReactiveSession(session, "Cassandra", DefaultCassandraObservationConvention.INSTANCE,
				observationRegistry);
	}

	/**
	 * Factory method for creation of a {@link ObservableReactiveSession}.
	 *
	 * @param session reactive session.
	 * @param remoteServiceName the remote service name.
	 * @param observationRegistry observation registry.
	 * @return traced representation of a {@link ReactiveSession}.
	 */
	public static ReactiveSession create(ReactiveSession session, String remoteServiceName,
			ObservationRegistry observationRegistry) {
		return new ObservableReactiveSession(session, remoteServiceName, DefaultCassandraObservationConvention.INSTANCE,
				observationRegistry);
	}

	/**
	 * Factory method for creation of a {@link ObservableReactiveSession}.
	 *
	 * @param session reactive session.
	 * @param remoteServiceName the remote service name.
	 * @param convention the observation convention.
	 * @param observationRegistry observation registry.
	 * @return traced representation of a {@link ReactiveSession}.
	 * @since 4.3.4
	 */
	public static ReactiveSession create(ReactiveSession session, String remoteServiceName,
			CassandraObservationConvention convention, ObservationRegistry observationRegistry) {
		return new ObservableReactiveSession(session, remoteServiceName, convention, observationRegistry);
	}

	@Override
	public boolean isClosed() {
		return this.delegate.isClosed();
	}

	@Override
	public DriverContext getContext() {
		return this.delegate.getContext();
	}

	@Override
	public Optional<CqlIdentifier> getKeyspace() {
		return this.delegate.getKeyspace();
	}

	@Override
	public Metadata getMetadata() {
		return this.delegate.getMetadata();
	}

	@Override
	public Mono<ReactiveResultSet> execute(String cql) {
		return execute(SimpleStatement.newInstance(cql));
	}

	@Override
	public Mono<ReactiveResultSet> execute(String cql, Object... objects) {
		return execute(SimpleStatement.newInstance(cql, objects));
	}

	@Override
	public Mono<ReactiveResultSet> execute(String cql, Map<String, Object> map) {
		return execute(SimpleStatement.newInstance(cql, map));
	}

	@Override
	public Mono<ReactiveResultSet> execute(Statement<?> statement) {

		if (ObservationStatement.isObservationStatement(statement)) {
			return this.delegate.execute(statement);
		}

		return Mono.deferContextual(contextView -> {

			Observation observation = startObservation(getParentObservation(contextView), statement, false, "execute");
			return this.delegate.execute(ObservationStatement.createProxy(observation, statement));
		});
	}

	@Override
	public Mono<PreparedStatement> prepare(String cql) {
		return prepare(SimpleStatement.newInstance(cql));
	}

	@Override
	public Mono<PreparedStatement> prepare(SimpleStatement statement) {

		if (ObservationStatement.isObservationStatement(statement)) {
			return this.delegate.prepare(statement);
		}

		// prepare calls do not notify RequestTracker so we need to stop the observation ourselves
		return Mono.deferContextual(contextView -> {

			Observation observation = startObservation(getParentObservation(contextView), statement, true, "prepare");
			return this.delegate.prepare(statement) //
					.doOnError(observation::error) //
					.doFinally(ignore -> observation.stop());
		});
	}

	@Override
	public void close() {
		this.delegate.close();
	}

	private Observation startObservation(@Nullable Observation parent, Statement<?> statement, boolean prepare,
			String methodName) {

		Observation observation = Observation
				.createNotStarted(methodName,
						() -> new CassandraObservationContext(statement, remoteServiceName, prepare, methodName,
								delegate.getContext().getSessionName(),
								delegate.getKeyspace().map(CqlIdentifier::asInternal).orElse("system")),
						observationRegistry)
				.observationConvention(convention);

		if (parent != null) {
			observation.parentObservation(parent);
		}

		return observation.start();
	}

	private static @Nullable Observation getParentObservation(ContextView contextView) {
		return contextView.getOrDefault(ObservationThreadLocalAccessor.KEY, null);
	}

}
