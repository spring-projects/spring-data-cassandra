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

import java.lang.reflect.Method;

import javax.annotation.Nonnull;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

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

	@Nullable
	@Override
	public Object invoke(@Nonnull MethodInvocation invocation) throws Throwable {

		Method method = invocation.getMethod();

		if (method.getName().equals("getTargetClass")) {
			return this.delegate.getClass();
		}

		if (method.getName().equals("getObservation")) {
			return this.observation;
		}

		Object result = invocation.proceed();
		if (result instanceof Statement<?>) {
			this.delegate = (Statement<?>) result;
		}
		return result;
	}

}
