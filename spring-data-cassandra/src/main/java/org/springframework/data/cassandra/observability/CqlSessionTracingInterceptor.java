/*
 * Copyright 2013-2022 the original author or authors.
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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
 * @since 4.0.0
 */
final class CqlSessionTracingInterceptor implements MethodInterceptor {

	private static final Log log = LogFactory.getLog(CqlSessionTracingInterceptor.class);

	private final CqlSession delegateSession;

	private final ObservationRegistry observationRegistry;

	private CqlSessionObservationConvention observationConvention;

	CqlSessionTracingInterceptor(CqlSession delegateSession, ObservationRegistry observationRegistry,
			CqlSessionObservationConvention observationConvention) {

		this.delegateSession = delegateSession;
		this.observationRegistry = observationRegistry;
		this.observationConvention = observationConvention;
	}

	@Nullable
	@Override
	public Object invoke(@NotNull MethodInvocation invocation) throws Throwable {

		Method method = invocation.getMethod();
		Object[] args = invocation.getArguments();

		if (method.getName().equals("execute") && args.length > 0) {
			return tracedCall(createStatement(args), method.getName(), this.delegateSession::execute);
		}

		if (method.getName().equals("executeAsync") && args.length > 0) {
			return tracedCall(createStatement(args), method.getName(), this.delegateSession::executeAsync);
		}

		if (method.getName().equals("prepare") && args.length > 0) {
			return tracedCall(createStatement(args), method.getName(),
					statement -> this.delegateSession.prepare((SimpleStatement) statement));
		}

		if (method.getName().equals("prepareAsync") && args.length > 0) {
			return tracedCall(createStatement(args), method.getName(),
					statement -> this.delegateSession.prepareAsync((SimpleStatement) statement));
		}

		return invocation.proceed();
	}

	/**
	 * Apply tracing to a {@link Statement}.
	 *
	 * @param statement original CQL {@link Statement}
	 * @param statementExecutor function that transforms a {@link Statement} into a resulting {@link Object}
	 * @return {@link ResultSet}
	 */
	private Object tracedCall(Statement<?> statement, String methodName,
			Function<Statement<?>, Object> statementExecutor) {

		if (this.observationRegistry.getCurrentObservation() == null) {
			return null;
		}

		Observation observation = childObservation(statement, methodName, this.delegateSession);

		if (log.isDebugEnabled()) {
			log.debug("Created a new child observation before query [" + observation + "]");
		}

		try (Observation.Scope scope = observation.openScope()) {
			return statementExecutor.apply(statement);
		} catch (Exception e) {
			observation.error(e);
			throw e;
		} finally {
			observation.stop();
		}
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

		if (args[0]instanceof String query && args.length == 2) {
			return args[1] instanceof Map //
					? SimpleStatement.newInstance(query, (Map) args[1]) //
					: SimpleStatement.newInstance(query, (Object[]) args[1]);
		}

		throw new IllegalArgumentException(String.format("Unsupported arguments %s", Arrays.toString(args)));
	}

	private Observation childObservation(Statement<?> statement, String methodName, CqlSession delegateSession) {

		CqlSessionContext observationContext = new CqlSessionContext(statement, methodName, delegateSession);

		return CassandraObservation.CASSANDRA_QUERY_OBSERVATION //
				.observation(this.observationRegistry, observationContext) //
				.contextualName(CassandraObservation.CASSANDRA_QUERY_OBSERVATION.getContextualName()) //
				.highCardinalityKeyValues(this.observationConvention.getHighCardinalityKeyValues(observationContext)) //
				.lowCardinalityKeyValues(this.observationConvention.getLowCardinalityKeyValues(observationContext)) //
				.start();
	}
}
