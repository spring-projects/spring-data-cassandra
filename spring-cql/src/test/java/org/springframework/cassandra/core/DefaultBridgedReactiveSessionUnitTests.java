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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Collections;

import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

import reactor.core.scheduler.Schedulers;

/**
 * Unit tests for {@link DefaultBridgedReactiveSession}.
 * 
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBridgedReactiveSessionUnitTests {

	@Mock private Session sessionMock;

	private DefaultBridgedReactiveSession reactiveSession;

	@Before
	public void before() throws Exception {
		reactiveSession = new DefaultBridgedReactiveSession(sessionMock, Schedulers.immediate());
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void executeStatementShouldForwardStatementToSession() throws Exception {

		SimpleStatement statement = new SimpleStatement("SELECT *");
		reactiveSession.execute(statement).subscribe();

		verify(sessionMock).executeAsync(statement);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void executeShouldForwardStatementToSession() throws Exception {

		reactiveSession.execute("SELECT *").subscribe();

		verify(sessionMock).executeAsync(eq(new SimpleStatement("SELECT *")));
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void executeWithValuesShouldForwardStatementToSession() throws Exception {

		reactiveSession.execute("SELECT * WHERE a = ? and b = ?", "A", "B").subscribe();

		verify(sessionMock).executeAsync(eq(new SimpleStatement("SELECT * WHERE a = ? and b = ?", "A", "B")));
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void executeWithValueMapShouldForwardStatementToSession() throws Exception {

		reactiveSession.execute("SELECT * WHERE a = ?", Collections.singletonMap("a", "value")).subscribe();

		verify(sessionMock)
				.executeAsync(eq(new SimpleStatement("SELECT * WHERE a = ?", Collections.singletonMap("a", "value"))));
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void testPrepareQuery() throws Exception {

		reactiveSession.prepare("SELECT *").subscribe();

		verify(sessionMock).prepareAsync(eq(new SimpleStatement("SELECT *")));
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void testPrepareStatement() throws Exception {

		SimpleStatement statement = new SimpleStatement("SELECT *");
		reactiveSession.prepare(statement).subscribe();

		verify(sessionMock).prepareAsync(statement);
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void testClose() throws Exception {

		reactiveSession.close();

		verify(sessionMock).close();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void testIsClosed() throws Exception {

		when(reactiveSession.isClosed()).thenReturn(true);

		boolean result = reactiveSession.isClosed();

		assertThat(result).isTrue();
		verify(sessionMock).isClosed();
	}

	/**
	 * @see DATACASS-335
	 */
	@Test
	public void testGetCluster() throws Exception {

		Cluster clusterMock = mock(Cluster.class);
		when(sessionMock.getCluster()).thenReturn(clusterMock);

		Cluster result = reactiveSession.getCluster();

		assertThat(result).isSameAs(clusterMock);
	}

	private static <T extends Statement> T eq(T value) {

		return Matchers.argThat(new IsEqual<T>(value) {

			@Override
			public boolean matches(Object actualValue) {

				if (actualValue instanceof Statement) {
					return value.toString().equals(actualValue.toString());
				}

				return super.matches(actualValue);
			}
		});
	}
}
