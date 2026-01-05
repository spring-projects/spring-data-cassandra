/*
 * Copyright 2022-present the original author or authors.
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

import java.lang.reflect.Method;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Trace implementation of a {@link Statement}.
 *
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @since 4.0
 */
final class ObservationStatement implements MethodInterceptor {

	private final Observation observation;

	private Statement<?> delegate;

	private ObservationStatement(Observation observation, Statement<?> delegate) {
		this.observation = observation;
		this.delegate = delegate;
	}

	/**
	 * Creates a proxy with {@link ObservationStatement} attached to it.
	 *
	 * @param observation current observation
	 * @param target target to proxy
	 * @param <T> type of target
	 * @return proxied object with trace advice
	 */
	@SuppressWarnings("unchecked")
	public static <T> T createProxy(Observation observation, T target) {

		ProxyFactory factory = new ProxyFactory(ClassUtils.getAllInterfaces(target));

		factory.addInterface(CassandraObservationSupplier.class);
		factory.setTarget(target);
		factory.addAdvice(new ObservationStatement(observation, (Statement<?>) target));

		return (T) factory.getProxy();
	}

	/**
	 * @param statement target statement to inspect
	 * @return whether the given {@code statement} is a traced statement wrapper
	 */
	public static boolean isObservationStatement(Statement<?> statement) {
		return statement instanceof CassandraObservationSupplier;
	}

	@Override
	public @Nullable Object invoke(MethodInvocation invocation) throws Throwable {

		Method method = invocation.getMethod();
		@Nullable
		Object[] args = invocation.getArguments();

		String name = method.getName();

		switch (name) {
			case "equals" -> {
				if (args.length == 1) {
					return equals(args[0]);
				}
			}
			case "hashCode" -> {
				return hashCode();
			}

			case "getTargetClass" -> {
				return this.delegate.getClass();
			}

			case "getObservation" -> {
				return this.observation;
			}
		}

		Object result = invocation.proceed();
		if (result instanceof Statement<?>) {
			this.delegate = (Statement<?>) result;
		}

		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof ObservationStatement that)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(delegate, that.delegate);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHash(delegate);
	}
}
