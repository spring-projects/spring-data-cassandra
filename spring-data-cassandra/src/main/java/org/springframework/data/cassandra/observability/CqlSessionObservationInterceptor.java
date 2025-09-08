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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;

import org.springframework.aop.TargetSource;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * A {@link MethodInterceptor} that wraps calls around {@link CqlSession} in a trace representation. This interceptor
 * wraps statements for {@code execute} and {@code prepare} (including their asynchronous variants) only. Graph and
 * reactive {@link CqlSession} method are called as-is.
 *
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 * @since 4.0
 */
final class CqlSessionObservationInterceptor implements MethodInterceptor {

	private final CqlSession delegate;

	private final String remoteServiceName;

	private final ObservationRegistry observationRegistry;

	private final CassandraObservationConvention convention;

	CqlSessionObservationInterceptor(CqlSession delegate, String remoteServiceName,
			CassandraObservationConvention convention, ObservationRegistry observationRegistry) {

		this.delegate = delegate;
		this.remoteServiceName = remoteServiceName;
		this.convention = convention;
		this.observationRegistry = observationRegistry;
	}

	@Override
	public @Nullable Object invoke(MethodInvocation invocation) throws Throwable {

		Method method = invocation.getMethod();
		Object[] args = invocation.getArguments();

		if (method.getName().equals("getTargetClass")) {
			return delegate.getClass();
		}

		if (method.getName().equals("execute") && args.length > 0) {
			return observe(createStatement(args), method.getName(), this.delegate::execute);
		}

		if (method.getName().equals("executeAsync") && args.length > 0) {
			return observe(createStatement(args), method.getName(), this.delegate::executeAsync);
		}

		// prepare calls do not notify RequestTracker so we need to stop the observation ourselves
		if (method.getName().equals("prepare") && args.length > 0) {

			Statement<?> statement = createStatement(args);

			if (ObservationStatement.isObservationStatement(statement)) {
				return this.delegate.prepareAsync((SimpleStatement) statement);
			}

			Observation observation = startObservation(statement, true, "prepare");

			try {
				return this.delegate.prepare((SimpleStatement) statement);
			} catch (RuntimeException e) {

				observation.error(e);
				throw e;
			} finally {
				observation.stop();
			}
		}

		if (method.getName().equals("prepareAsync") && args.length > 0) {

			Statement<?> statement = createStatement(args);

			if (ObservationStatement.isObservationStatement(statement)) {
				return this.delegate.prepareAsync((SimpleStatement) statement);
			}

			Observation observation = startObservation(statement, true, "prepareAsync");

			return this.delegate.prepareAsync((SimpleStatement) statement)
					.whenComplete((preparedStatement, throwable) -> {

						if (throwable != null) {
							observation.error(throwable);
						}

						observation.stop();
					});
		}

		return invocation.proceed();
	}

	/**
	 * Convert list of arguments into a {@link Statement}.
	 *
	 * @param args
	 * @return CQL statement
	 */
	private static Statement<?> createStatement(Object[] args) {

		if (args[0] instanceof Statement) {
			return (Statement<?>) args[0];
		}

		if (args[0] instanceof String & args.length == 1) {
			return SimpleStatement.newInstance((String) args[0]);
		}

		if (args[0] instanceof String query && args.length == 2) {
			return args[1] instanceof Map //
					? SimpleStatement.newInstance(query, (Map) args[1]) //
					: SimpleStatement.newInstance(query, (Object[]) args[1]);
		}

		throw new IllegalArgumentException(String.format("Unsupported arguments %s", Arrays.toString(args)));
	}

	/**
	 * Observe a {@link Statement}.
	 *
	 * @param statement original CQL {@link Statement}
	 * @param statementExecutor function that transforms a {@link Statement} into a resulting {@link Object}
	 * @return the statement execution result.
	 */
	private Object observe(Statement<?> statement, String methodName, Function<Statement<?>, Object> statementExecutor) {

		// avoid duplicate statement wrapping
		if (ObservationStatement.isObservationStatement(statement)) {
			return statementExecutor.apply(statement);
		}

		Statement<?> observableStatement = ObservationStatement.createProxy(startObservation(statement, false, methodName),
				statement);

		return statementExecutor.apply(observableStatement);
	}

	private Observation startObservation(Statement<?> statement, boolean prepare, String methodName) {

		Observation currentObservation = observationRegistry.getCurrentObservation();

		Observation observation = Observation
				.createNotStarted(methodName,
						() -> new CassandraObservationContext(statement, remoteServiceName, prepare, methodName,
								delegate.getContext().getSessionName(),
								delegate.getKeyspace().map(CqlIdentifier::asInternal).orElse("system")),
						observationRegistry)
				.observationConvention(convention);

		if (currentObservation != null) {
			observation.parentObservation(currentObservation);
		}

		return observation.start();
	}

	/**
	 * Marker interface for components that want to participate in observation but do not want to work with a
	 * {@code CqlSession} that is already decorated for observation.
	 */
	public interface ObservationDecoratedProxy extends TargetSource {

	}

}
