/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import reactor.core.scheduler.Schedulers;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Unit tests for {@link DefaultBridgedReactiveSession}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBridgedReactiveSessionUnitTests {

	@Mock Session sessionMock;
	@Mock ResultSetFuture future;
	@Mock ListenableFuture<PreparedStatement> preparedStatementFuture;

	private DefaultBridgedReactiveSession reactiveSession;

	@Before
	public void before() throws Exception {

		reactiveSession = new DefaultBridgedReactiveSession(sessionMock, Schedulers.immediate());

		when(sessionMock.executeAsync(any(Statement.class))).thenReturn(future);
		when(sessionMock.prepareAsync(any(RegularStatement.class))).thenReturn(preparedStatementFuture);
	}

	@Test // DATACASS-335
	public void executeStatementShouldForwardStatementToSession() throws Exception {

		SimpleStatement statement = new SimpleStatement("SELECT *");

		reactiveSession.execute(statement).subscribe();

		verify(sessionMock).executeAsync(statement);
	}

	@Test // DATACASS-335
	public void executeShouldForwardStatementToSession() throws Exception {

		reactiveSession.execute("SELECT *").subscribe();

		verify(sessionMock).executeAsync(eq(new SimpleStatement("SELECT *")));
	}

	@Test // DATACASS-335
	public void executeWithValuesShouldForwardStatementToSession() throws Exception {

		reactiveSession.execute("SELECT * WHERE a = ? and b = ?", "A", "B").subscribe();

		verify(sessionMock).executeAsync(eq(new SimpleStatement("SELECT * WHERE a = ? and b = ?", "A", "B")));
	}

	@Test // DATACASS-335
	public void executeWithValueMapShouldForwardStatementToSession() throws Exception {

		reactiveSession.execute("SELECT * WHERE a = ?", Collections.singletonMap("a", "value")).subscribe();

		verify(sessionMock)
				.executeAsync(eq(new SimpleStatement("SELECT * WHERE a = ?", Collections.singletonMap("a", "value"))));
	}

	@Test // DATACASS-335
	public void testPrepareQuery() throws Exception {

		reactiveSession.prepare("SELECT *").subscribe();

		verify(sessionMock).prepareAsync(eq(new SimpleStatement("SELECT *")));
	}

	@Test // DATACASS-335
	public void testPrepareStatement() throws Exception {

		SimpleStatement statement = new SimpleStatement("SELECT *");
		reactiveSession.prepare(statement).subscribe();

		verify(sessionMock).prepareAsync(statement);
	}

	@Test // DATACASS-335
	public void testClose() throws Exception {

		reactiveSession.close();

		verify(sessionMock).close();
	}

	@Test // DATACASS-335
	public void testIsClosed() throws Exception {

		when(reactiveSession.isClosed()).thenReturn(true);

		boolean result = reactiveSession.isClosed();

		assertThat(result).isTrue();
		verify(sessionMock).isClosed();
	}

	@Test // DATACASS-335
	public void testGetCluster() throws Exception {

		Cluster clusterMock = mock(Cluster.class);
		when(sessionMock.getCluster()).thenReturn(clusterMock);

		Cluster result = reactiveSession.getCluster();

		assertThat(result).isSameAs(clusterMock);
	}

	private static <T extends Statement> T eq(T value) {

		return ArgumentMatchers.argThat(argument -> argument instanceof Statement //
				? value.toString().equals(argument.toString()) //
				: value.equals(argument));
	}
}
