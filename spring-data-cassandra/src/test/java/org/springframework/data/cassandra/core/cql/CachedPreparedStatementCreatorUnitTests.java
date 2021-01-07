/*
 * Copyright 2016-2021 the original author or authors.
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

import static edu.umd.cs.mtc.TestFramework.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.mtc.MultithreadedTestCase;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit tests for {@link CachedPreparedStatementCreator}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachedPreparedStatementCreatorUnitTests {

	private PreparedStatement preparedStatement;
	@Mock CqlSession sessionMock;

	@BeforeEach
	void before() throws Exception {

		preparedStatement = newProxy(PreparedStatement.class, new TestInvocationHandler());
		when(sessionMock.prepare(anyString())).thenReturn(preparedStatement);
	}

	@Test // DATACASS-253
	void shouldRejectEmptyCql() {
		assertThatIllegalArgumentException().isThrownBy(() -> new CachedPreparedStatementCreator(""));
	}

	@Test // DATACASS-253
	void shouldRejectNullCql() {
		assertThatIllegalArgumentException().isThrownBy(() -> new CachedPreparedStatementCreator(null));
	}

	@Test // DATACASS-253
	void shouldCreatePreparedStatement() {

		CachedPreparedStatementCreator cachedPreparedStatementCreator = new CachedPreparedStatementCreator("my cql");

		PreparedStatement result = cachedPreparedStatementCreator.createPreparedStatement(sessionMock);

		assertThat(result).isSameAs(preparedStatement);
		verify(sessionMock).prepare("my cql");
	}

	@Test // DATACASS-253
	void shouldCacheCreatePreparedStatement() {

		CachedPreparedStatementCreator cachedPreparedStatementCreator = new CachedPreparedStatementCreator("my cql");

		cachedPreparedStatementCreator.createPreparedStatement(sessionMock);
		cachedPreparedStatementCreator.createPreparedStatement(sessionMock);

		PreparedStatement result = cachedPreparedStatementCreator.createPreparedStatement(sessionMock);

		assertThat(result).isSameAs(preparedStatement);
		verify(sessionMock, times(1)).prepare("my cql");
	}

	@Test // DATACASS-253
	void concurrentAccessToCreateStatementShouldBeSynchronized() throws Throwable {

		CreatePreparedStatementIsThreadSafe concurrentPrepareStatement = new CreatePreparedStatementIsThreadSafe(
				preparedStatement, new CachedPreparedStatementCreator("my cql"));

		runManyTimes(concurrentPrepareStatement, 5);
	}

	@SuppressWarnings("unused")
	private static class CreatePreparedStatementIsThreadSafe extends MultithreadedTestCase {

		final AtomicInteger atomicInteger = new AtomicInteger();
		private final CachedPreparedStatementCreator preparedStatementCreator;
		private final CqlSession session;

		private CreatePreparedStatementIsThreadSafe(final PreparedStatement preparedStatement,
				CachedPreparedStatementCreator preparedStatementCreator) {

			this.preparedStatementCreator = preparedStatementCreator;

			this.session = newProxy(CqlSession.class, new TestInvocationHandler() {

				@Override
				public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

					if (method.getName().equals("prepare") && args.length == 1) {
						waitForTick(2);
						atomicInteger.incrementAndGet();
						return preparedStatement;
					}

					if (method.getName().equals("getKeyspace")) {
						return Optional.of(CqlIdentifier.fromCql("system"));
					}

					return super.invoke(proxy, method, args);
				}
			});
		}

		public void thread1() {

			waitForTick(1);

			preparedStatementCreator.createPreparedStatement(session);

			assertThat(atomicInteger.get()).isEqualTo(1);
		}

		public void thread2() {

			waitForTick(1);

			preparedStatementCreator.createPreparedStatement(session);

			assertThat(atomicInteger.get()).isEqualTo(1);
		}
	}

	private static class TestInvocationHandler implements InvocationHandler {

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

			if (method.getName().equals("hashCode")) {
				return hashCode();
			}

			if (method.getName().equals("equals") && args.length == 1) {
				return equals(args[0]);
			}

			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> T newProxy(Class<T> theClass, InvocationHandler invocationHandler) {
		return (T) Proxy.newProxyInstance(CachedPreparedStatementCreatorUnitTests.class.getClassLoader(),
				new Class[] { theClass }, invocationHandler);
	}
}
