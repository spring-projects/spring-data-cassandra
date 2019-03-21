/*
 *  Copyright 2013-2016 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cassandra.config;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.CqlOperations;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * The CassandraCqlSessionFactoryBeanUnitTests class is a test suite of test cases testing the contract
 * and functionality of the {@link CassandraCqlSessionFactoryBean} class.
 *
 * @author John Blum
 * @see org.springframework.cassandra.config.CassandraCqlSessionFactoryBean
 * @see <a href="https://jira.spring.io/browse/DATACASS-219>DATACASS-219</a>
 * @since 1.5.0
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraCqlSessionFactoryBeanUnitTests {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private Cluster mockCluster;

	@Mock
	private Session mockSession;

	private CassandraCqlSessionFactoryBean factoryBean;

	protected <T> List<T> asList(T... array) {
		return new ArrayList<T>(Arrays.asList(array));
	}

	protected void assertNonNullEmptyCollection(Collection<?> collection) {
		assertThat(collection, is(notNullValue()));
		assertThat(collection.isEmpty(), is(true));
	}

	@Before
	public void setup() {
		factoryBean = spy(new CassandraCqlSessionFactoryBean());
	}

	@Test
	public void cassandraCqlSessionFactoryBeanIsSingleton() {
		assertThat(factoryBean.isSingleton(), is(true));
	}

	@Test
	public void objectTypeWhenSessionHasNotBeenInitializedIsSessionClass() {
		assertThat(factoryBean.getObject(), is(nullValue()));
		assertEquals(Session.class, factoryBean.<Session>getObjectType());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void afterPropertiesSetInitializesSessionWithKeyspaceAndExecutesStartupScripts() throws Exception {
		List<String> expectedStartupScripts = asList("/path/to/schema.cql", "/path/to/data.cql");

		CqlOperations mockCqlOperations = mock(CqlOperations.class);

		doReturn(mockSession).when(factoryBean).connect(eq("TestKeyspace"));
		doReturn(mockCqlOperations).when(factoryBean).newCqlOperations(eq(mockSession));

		factoryBean.setKeyspaceName("TestKeyspace");
		factoryBean.setStartupScripts(expectedStartupScripts);

		assertThat(factoryBean.getKeyspaceName(), is(equalTo("TestKeyspace")));
		assertThat(factoryBean.getStartupScripts(), is(equalTo(expectedStartupScripts)));

		factoryBean.afterPropertiesSet();

		assertEquals(mockSession.getClass(), factoryBean.getObjectType());
		assertThat(factoryBean.getObject(), is(equalTo(mockSession)));
		assertThat(factoryBean.getSession(), is(equalTo(mockSession)));

		InOrder inOrder = inOrder(factoryBean);

		inOrder.verify(factoryBean, times(1)).connect(eq("TestKeyspace"));
		inOrder.verify(factoryBean, times(1)).executeScripts(eq(expectedStartupScripts));
		inOrder.verify(factoryBean, times(1)).newCqlOperations(eq(mockSession));
		verify(mockCqlOperations, times(1)).execute(eq(expectedStartupScripts.get(0)));
		verify(mockCqlOperations, times(1)).execute(eq(expectedStartupScripts.get(1)));
	}

	@Test
	public void connectToSystemKeyspace() {
		when(mockCluster.connect()).thenReturn(mockSession);

		factoryBean.setCluster(mockCluster);

		assertThat(factoryBean.connect(null), is(equalTo(mockSession)));

		verify(mockCluster, times(1)).connect();
		verify(mockCluster, never()).connect(anyString());
	}

	@Test
	public void connectToTargetKeyspace() {
		when(mockCluster.connect(eq("TestKeyspace"))).thenReturn(mockSession);

		factoryBean.setCluster(mockCluster);

		assertThat(factoryBean.connect("TestKeyspace"), is(equalTo(mockSession)));

		verify(mockCluster, never()).connect();
		verify(mockCluster, times(1)).connect(eq("TestKeyspace"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void destroySessionAndExecutesShutdownScripts() throws Exception {
		List<String> expectedShutdownScripts = asList("/path/to/shutdown.cql");

		CqlOperations mockCqlOperations = mock(CqlOperations.class);

		doReturn(mockSession).when(factoryBean).getSession();
		doReturn(mockCqlOperations).when(factoryBean).newCqlOperations(eq(mockSession));

		factoryBean.setShutdownScripts(expectedShutdownScripts);
		factoryBean.destroy();

		InOrder inOrder = inOrder(factoryBean, mockSession);

		inOrder.verify(factoryBean, times(1)).executeScripts(eq(expectedShutdownScripts));
		verify(mockCqlOperations, times(1)).execute(eq(expectedShutdownScripts.get(0)));
		inOrder.verify(mockSession, times(1)).close();
	}

	@Test
	public void isConnectedWithNullSessionIsFalse() {
		assertThat(factoryBean.getObject(), is(nullValue()));
		assertThat(factoryBean.isConnected(), is(false));
	}

	@Test
	public void isConnectedWithClosedSessionIsFalse() {
		doReturn(mockSession).when(factoryBean).getObject();
		when(mockSession.isClosed()).thenReturn(true);
		assertThat(factoryBean.isConnected(), is(false));
		verify(mockSession, times(1)).isClosed();
	}

	@Test
	public void isConnectedWithOpenSessionIsTrue() {
		doReturn(mockSession).when(factoryBean).getObject();
		when(mockSession.isClosed()).thenReturn(false);
		assertThat(factoryBean.isConnected(), is(true));
		verify(mockSession, times(1)).isClosed();
	}

	@Test
	public void setAndGetCluster() {
		factoryBean.setCluster(mockCluster);
		assertThat(factoryBean.getCluster(), is(equalTo(mockCluster)));
	}

	@Test
	public void setClusterToNullThrowsIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("Cluster must not be null");

		factoryBean.setCluster(null);
	}

	@Test
	public void getClusterWhenUninitializedThrowsIllegalStateException() {
		exception.expect(IllegalStateException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("Cluster was not properly initialized");

		factoryBean.getCluster();
	}

	@Test
	public void setAndGetKeyspaceName() {
		assertThat(factoryBean.getKeyspaceName(), is(nullValue()));

		factoryBean.setKeyspaceName("TEST");

		assertThat(factoryBean.getKeyspaceName(), is(equalTo("TEST")));

		factoryBean.setKeyspaceName(null);

		assertThat(factoryBean.getKeyspaceName(), is(nullValue()));
	}

	@Test
	public void getSessionWhenUninitializedThrowsIllegalStateException() {
		exception.expect(IllegalStateException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage(is(equalTo("Session was not properly initialized")));

		assertThat(factoryBean.getObject(), is(nullValue()));

		factoryBean.getSession();
	}

	@Test
	public void setAndGetStartupScripts() {
		assertNonNullEmptyCollection(factoryBean.getStartupScripts());

		List<String> expectedStartupScripts = asList("/path/to/schema.cql", "/path/to/data.cql");

		factoryBean.setStartupScripts(expectedStartupScripts);

		List<String> actualStartupScripts = factoryBean.getStartupScripts();

		assertThat(actualStartupScripts, is(not(sameInstance(expectedStartupScripts))));
		assertThat(actualStartupScripts, is(equalTo(expectedStartupScripts)));

		factoryBean.setStartupScripts(null);

		assertNonNullEmptyCollection(factoryBean.getStartupScripts());
	}

	@Test
	public void startupScriptsAreImmutable() {
		List<String> startupScripts = asList("/path/to/startup.cql");

		factoryBean.setStartupScripts(startupScripts);

		List<String> actualStartupScripts = factoryBean.getStartupScripts();

		assertThat(actualStartupScripts, is(notNullValue()));
		assertThat(actualStartupScripts, is(not(sameInstance(startupScripts))));
		assertThat(actualStartupScripts, is(equalTo(startupScripts)));

		startupScripts.add("/path/to/another.cql");

		actualStartupScripts = factoryBean.getStartupScripts();

		assertThat(actualStartupScripts, is(not(equalTo(startupScripts))));
		assertThat(actualStartupScripts.size(), is(equalTo(1)));
		assertThat(actualStartupScripts.get(0), is(equalTo(startupScripts.get(0))));

		try {
			exception.expect(UnsupportedOperationException.class);
			actualStartupScripts.add("/path/to/yetAnother.cql");
		} finally {
			assertThat(actualStartupScripts.size(), is(equalTo(1)));
		}
	}

	@Test
	public void setAndGetShutdownScripts() {
		assertNonNullEmptyCollection(factoryBean.getShutdownScripts());

		List<String> expectedShutdownScripts = asList("/path/to/backup.cql", "/path/to/dropTables.cql");

		factoryBean.setShutdownScripts(expectedShutdownScripts);

		List<String> actualShutdownScripts = factoryBean.getShutdownScripts();

		assertThat(actualShutdownScripts, is(not(sameInstance(expectedShutdownScripts))));
		assertThat(actualShutdownScripts, is(equalTo(expectedShutdownScripts)));

		factoryBean.setShutdownScripts(null);

		assertNonNullEmptyCollection(factoryBean.getShutdownScripts());
	}

	@Test
	public void shutdownScriptsAreImmutable() {
		List<String> shutdownScripts = asList("/path/to/shutdown.cql");

		factoryBean.setShutdownScripts(shutdownScripts);

		List<String> actualShutdownScripts = factoryBean.getShutdownScripts();

		assertThat(actualShutdownScripts, is(notNullValue()));
		assertThat(actualShutdownScripts, is(not(sameInstance(shutdownScripts))));
		assertThat(actualShutdownScripts, is(equalTo(shutdownScripts)));

		shutdownScripts.add("/path/to/corruptSession.cql");

		actualShutdownScripts = factoryBean.getShutdownScripts();

		assertThat(actualShutdownScripts, is(not(sameInstance(shutdownScripts))));
		assertThat(actualShutdownScripts, is(not(equalTo(shutdownScripts))));
		assertThat(actualShutdownScripts.size(), is(equalTo(1)));

		try {
			exception.expect(UnsupportedOperationException.class);
			actualShutdownScripts.add("/path/to/blowUpCluster.cql");
		} finally {
			assertThat(actualShutdownScripts.size(), is(equalTo(1)));
		}
	}
}
