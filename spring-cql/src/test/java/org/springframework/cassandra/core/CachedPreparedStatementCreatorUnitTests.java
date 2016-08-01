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

import static edu.umd.cs.mtc.TestFramework.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import edu.umd.cs.mtc.MultithreadedTestCase;

/**
 * Unit tests for {@link CachedPreparedStatementCreator}.
 * 
 * @author Mark Paluch
 * @see <a href="https://jira.spring.io/browse/DATACASS-253">DATACASS-253</a>
 */
@RunWith(MockitoJUnitRunner.class)
public class CachedPreparedStatementCreatorUnitTests {

	PreparedStatement preparedStatement;
	@Mock Session sessionMock;

	@Before
	public void before() throws Exception {

		preparedStatement = newProxy(PreparedStatement.class, new TestInvocationHandler());
		when(sessionMock.prepare(anyString())).thenReturn(preparedStatement);
	}

	/**
	 * @see DATACASS-253
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldRejectEmptyCql() {
		new CachedPreparedStatementCreator("");
	}

	/**
	 * @see DATACASS-253
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldRejectNullCql() {
		new CachedPreparedStatementCreator(null);
	}

	/**
	 * @see DATACASS-253
	 */
	@Test
	public void shouldCreatePreparedStatement() {

		CachedPreparedStatementCreator cachedPreparedStatementCreator = new CachedPreparedStatementCreator("my cql");

		PreparedStatement result = cachedPreparedStatementCreator.createPreparedStatement(sessionMock);

		assertThat(result).isSameAs(preparedStatement);
		verify(sessionMock).prepare("my cql");
	}

	/**
	 * @see DATACASS-253
	 */
	@Test
	public void shouldCacheCreatePreparedStatement() {

		CachedPreparedStatementCreator cachedPreparedStatementCreator = new CachedPreparedStatementCreator("my cql");

		cachedPreparedStatementCreator.createPreparedStatement(sessionMock);
		cachedPreparedStatementCreator.createPreparedStatement(sessionMock);

		PreparedStatement result = cachedPreparedStatementCreator.createPreparedStatement(sessionMock);

		assertThat(result).isSameAs(preparedStatement);
		verify(sessionMock, times(1)).prepare("my cql");
	}

	/**
	 * @see DATACASS-253
	 * @throws Throwable
	 */
	@Test
	public void concurrentAccessToCreateStatementShouldBeSynchronized() throws Throwable {

		CreatePreparedStatementIsThreadSafe concurrentPrepareStatement = new CreatePreparedStatementIsThreadSafe(
				preparedStatement, new CachedPreparedStatementCreator("my cql"));

		runManyTimes(concurrentPrepareStatement, 5);
	}

	@SuppressWarnings("unused")
	private static class CreatePreparedStatementIsThreadSafe extends MultithreadedTestCase {

		final AtomicInteger atomicInteger = new AtomicInteger();
		final CachedPreparedStatementCreator preparedStatementCreator;
		final Session session;

		public CreatePreparedStatementIsThreadSafe(final PreparedStatement preparedStatement,
				CachedPreparedStatementCreator preparedStatementCreator) {

			this.preparedStatementCreator = preparedStatementCreator;

			this.session = newProxy(Session.class, new TestInvocationHandler() {

				@Override
				public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

					if (method.getName().equals("prepare") && args.length == 1) {
						waitForTick(2);
						atomicInteger.incrementAndGet();
						return preparedStatement;
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
