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
package org.springframework.cassandra.config;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.same;

import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;

/**
 * Unit tests for {@link PoolingOptionsFactoryBean}.
 *
 * @author Sumit Kumar
 * @author David Webb
 * @author John Blum
 * @see org.springframework.cassandra.config.PoolingOptionsFactoryBean
 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-176</a>
 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
 */
@RunWith(MockitoJUnitRunner.class)
public class PoolingOptionsFactoryBeanUnitTests {

	@Mock
	private Executor mockExecutor;

	@Spy
	private PoolingOptions poolingOptionsSpy;

	private PoolingOptionsFactoryBean poolingOptionsFactoryBean;

	@Before
	public void setup() {
		poolingOptionsFactoryBean = new PoolingOptionsFactoryBean();
	}

	@Test
	public void getObjectReturnsNullWhenNotInitialized() throws Exception {
		assertThat(poolingOptionsFactoryBean.getObject(), is(nullValue(PoolingOptions.class)));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void getObjectTypeReturnsPoolingOptionsClassWhenNotInitialized() {
		assertThat((Class<PoolingOptions>) poolingOptionsFactoryBean.getObjectType(), is(equalTo(PoolingOptions.class)));
	}

	@Test
	public void isSingletonIsTrue() {
		assertThat(poolingOptionsFactoryBean.isSingleton(), is(true));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void setAndGetFactoryBeanProperties() {
		poolingOptionsFactoryBean.setHeartbeatIntervalSeconds(15);
		poolingOptionsFactoryBean.setIdleTimeoutSeconds(120);
		poolingOptionsFactoryBean.setInitializationExecutor(mockExecutor);
		poolingOptionsFactoryBean.setLocalCoreConnections(50);
		poolingOptionsFactoryBean.setLocalMaxConnections(1000);
		poolingOptionsFactoryBean.setLocalMaxSimultaneousRequests(200);
		poolingOptionsFactoryBean.setLocalMinSimultaneousRequests(100);
		poolingOptionsFactoryBean.setPoolTimeoutMilliseconds(300);
		poolingOptionsFactoryBean.setRemoteCoreConnections(25);
		poolingOptionsFactoryBean.setRemoteMaxConnections(250);
		poolingOptionsFactoryBean.setRemoteMaxSimultaneousRequests(100);
		poolingOptionsFactoryBean.setRemoteMinSimultaneousRequests(50);

		assertThat(poolingOptionsFactoryBean.getHeartbeatIntervalSeconds(), is(equalTo(15)));
		assertThat(poolingOptionsFactoryBean.getIdleTimeoutSeconds(), is(equalTo(120)));
		assertThat(poolingOptionsFactoryBean.getInitializationExecutor(), is(equalTo(mockExecutor)));
		assertThat(poolingOptionsFactoryBean.getLocalCoreConnections(), is(equalTo(50)));
		assertThat(poolingOptionsFactoryBean.getLocalMaxConnections(), is(equalTo(1000)));
		assertThat(poolingOptionsFactoryBean.getLocalMaxSimultaneousRequests(), is(equalTo(200)));
		assertThat(poolingOptionsFactoryBean.getLocalMinSimultaneousRequests(), is(equalTo(100)));
		assertThat(poolingOptionsFactoryBean.getPoolTimeoutMilliseconds(), is(equalTo(300)));
		assertThat(poolingOptionsFactoryBean.getRemoteCoreConnections(), is(equalTo(25)));
		assertThat(poolingOptionsFactoryBean.getRemoteMaxConnections(), is(equalTo(250)));
		assertThat(poolingOptionsFactoryBean.getRemoteMaxSimultaneousRequests(), is(equalTo(100)));
		assertThat(poolingOptionsFactoryBean.getRemoteMinSimultaneousRequests(), is(equalTo(50)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void afterPropertiesSetInitializesLocalPoolingOptions() throws Exception {
		PoolingOptionsFactoryBean poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override PoolingOptions newPoolingOptions() {
				poolingOptionsSpy.setNewConnectionThreshold(HostDistance.LOCAL, 1);
				return poolingOptionsSpy;
			}
		};

		poolingOptionsFactoryBean.setHeartbeatIntervalSeconds(60);
		poolingOptionsFactoryBean.setIdleTimeoutSeconds(300);
		poolingOptionsFactoryBean.setInitializationExecutor(mockExecutor);
		poolingOptionsFactoryBean.setLocalCoreConnections(10);
		poolingOptionsFactoryBean.setLocalMaxConnections(100);
		poolingOptionsFactoryBean.setLocalMaxSimultaneousRequests(50);
		poolingOptionsFactoryBean.setLocalMinSimultaneousRequests(5);
		poolingOptionsFactoryBean.setPoolTimeoutMilliseconds(180);

		assertThat(poolingOptionsFactoryBean.getObject(), is(nullValue(PoolingOptions.class)));

		poolingOptionsFactoryBean.afterPropertiesSet();

		assertThat(poolingOptionsFactoryBean.getObject(), is(sameInstance(poolingOptionsSpy)));
		assertThat(poolingOptionsFactoryBean.getObjectType(), is(equalTo((Class) poolingOptionsSpy.getClass())));

		verify(poolingOptionsSpy, times(1)).setHeartbeatIntervalSeconds(eq(60));
		verify(poolingOptionsSpy, times(1)).setIdleTimeoutSeconds(eq(300));
		verify(poolingOptionsSpy, times(1)).setInitializationExecutor(eq(mockExecutor));
		verify(poolingOptionsSpy, times(1)).setPoolTimeoutMillis(eq(180));
		verify(poolingOptionsSpy, times(1)).setCoreConnectionsPerHost(eq(HostDistance.LOCAL), eq(10));
		verify(poolingOptionsSpy, times(1)).setMaxConnectionsPerHost(eq(HostDistance.LOCAL), eq(100));
		verify(poolingOptionsSpy, times(1)).setMaxRequestsPerConnection(eq(HostDistance.LOCAL), eq(50));
		verify(poolingOptionsSpy, times(1)).setNewConnectionThreshold(eq(HostDistance.LOCAL), eq(5));
		verify(poolingOptionsSpy, never()).setCoreConnectionsPerHost(eq(HostDistance.REMOTE), anyInt());
		verify(poolingOptionsSpy, never()).setMaxConnectionsPerHost(eq(HostDistance.REMOTE), anyInt());
		verify(poolingOptionsSpy, never()).setMaxRequestsPerConnection(eq(HostDistance.REMOTE), anyInt());
		verify(poolingOptionsSpy, never()).setNewConnectionThreshold(eq(HostDistance.REMOTE), anyInt());
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-298">DATACASS-298</a>
	 */
	@Test
	public void afterPropertiesSetInitializesRemotePoolingOptions() throws Exception {
		PoolingOptionsFactoryBean poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override PoolingOptions newPoolingOptions() {
				poolingOptionsSpy.setNewConnectionThreshold(HostDistance.REMOTE, 10);
				return poolingOptionsSpy;
			}
		};

		poolingOptionsFactoryBean.setHeartbeatIntervalSeconds(30);
		poolingOptionsFactoryBean.setIdleTimeoutSeconds(120);
		poolingOptionsFactoryBean.setInitializationExecutor(mockExecutor);
		poolingOptionsFactoryBean.setPoolTimeoutMilliseconds(120);
		poolingOptionsFactoryBean.setRemoteCoreConnections(5);
		poolingOptionsFactoryBean.setRemoteMaxConnections(50);
		poolingOptionsFactoryBean.setRemoteMaxSimultaneousRequests(20);
		poolingOptionsFactoryBean.setRemoteMinSimultaneousRequests(5);

		assertThat(poolingOptionsFactoryBean.getObject(), is(nullValue(PoolingOptions.class)));

		poolingOptionsFactoryBean.afterPropertiesSet();

		assertThat(poolingOptionsFactoryBean.getObject(), is(sameInstance(poolingOptionsSpy)));
		assertThat(poolingOptionsFactoryBean.getObjectType(), is(equalTo((Class) poolingOptionsSpy.getClass())));

		verify(poolingOptionsSpy, times(1)).setHeartbeatIntervalSeconds(eq(30));
		verify(poolingOptionsSpy, times(1)).setIdleTimeoutSeconds(eq(120));
		verify(poolingOptionsSpy, times(1)).setInitializationExecutor(eq(mockExecutor));
		verify(poolingOptionsSpy, times(1)).setPoolTimeoutMillis(eq(120));
		verify(poolingOptionsSpy, times(1)).setCoreConnectionsPerHost(eq(HostDistance.REMOTE), eq(5));
		verify(poolingOptionsSpy, times(1)).setMaxConnectionsPerHost(eq(HostDistance.REMOTE), eq(50));
		verify(poolingOptionsSpy, times(1)).setMaxRequestsPerConnection(eq(HostDistance.REMOTE), eq(20));
		verify(poolingOptionsSpy, never()).setCoreConnectionsPerHost(eq(HostDistance.LOCAL), anyInt());
		verify(poolingOptionsSpy, never()).setMaxConnectionsPerHost(eq(HostDistance.LOCAL), anyInt());
		verify(poolingOptionsSpy, never()).setMaxRequestsPerConnection(eq(HostDistance.LOCAL), anyInt());
		verify(poolingOptionsSpy, never()).setNewConnectionThreshold(eq(HostDistance.LOCAL), anyInt());
		verify(poolingOptionsSpy, never()).setNewConnectionThreshold(eq(HostDistance.REMOTE), eq(5));
	}

	/**
	 * This particular test case is technically an integration test since it uses an actual instance of
	 * a DataStax Java driver class type... {@link PoolingOptions}!
	 *
	 * The max values should be set before setting core values. Otherwise the core values will be compared with the
	 * default max values which is 8. Same for other min-max properties pairs. This test checks the same.
	 *
	 * @throws Exception Any unhandled scenarios will result in a test failure.
	 * @see <a href="https://jira.spring.io/browse/DATACASS-176">DATACASS-176</a>
	 */
	@Test
	public void afterPropertiesSetProperlySetsPoolingOptionsMaxBeforeMinProperties() throws Exception {
		poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override PoolingOptions newPoolingOptions() {
				return spy(super.newPoolingOptions());
			}
		};

		poolingOptionsFactoryBean.setLocalMaxConnections(200);
		poolingOptionsFactoryBean.setLocalCoreConnections(100);
		poolingOptionsFactoryBean.setLocalMaxSimultaneousRequests(99);
		poolingOptionsFactoryBean.setLocalMinSimultaneousRequests(97);
		poolingOptionsFactoryBean.setRemoteMaxConnections(210);
		poolingOptionsFactoryBean.setRemoteCoreConnections(110);
		poolingOptionsFactoryBean.setRemoteMaxSimultaneousRequests(127);
		poolingOptionsFactoryBean.setRemoteMinSimultaneousRequests(111);

		assertThat(poolingOptionsFactoryBean.getObject(), is(nullValue(PoolingOptions.class)));

		poolingOptionsFactoryBean.afterPropertiesSet();

		PoolingOptions poolingOptions = poolingOptionsFactoryBean.getObject();

		assertThat(poolingOptions, is(notNullValue(PoolingOptions.class)));
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL), is(equalTo(100)));
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL), is(equalTo(200)));
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL), is(equalTo(99)));
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.LOCAL), is(equalTo(97)));
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE), is(equalTo(110)));
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE), is(equalTo(210)));
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE), is(equalTo(127)));
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.REMOTE), is(equalTo(111)));

		InOrder inOrder = inOrder(poolingOptions);

		inOrder.verify(poolingOptions, times(1)).setMaxConnectionsPerHost(eq(HostDistance.LOCAL), eq(200));
		inOrder.verify(poolingOptions, times(1)).setCoreConnectionsPerHost(eq(HostDistance.LOCAL), eq(100));
		inOrder.verify(poolingOptions, times(1)).setMaxRequestsPerConnection(eq(HostDistance.LOCAL), eq(99));
		inOrder.verify(poolingOptions, times(1)).setNewConnectionThreshold(eq(HostDistance.LOCAL), eq(97));
		inOrder.verify(poolingOptions, times(1)).setMaxConnectionsPerHost(eq(HostDistance.REMOTE), eq(210));
		inOrder.verify(poolingOptions, times(1)).setCoreConnectionsPerHost(eq(HostDistance.REMOTE), eq(110));
		inOrder.verify(poolingOptions, times(1)).setMaxRequestsPerConnection(eq(HostDistance.REMOTE), eq(127));
		inOrder.verify(poolingOptions, times(1)).setNewConnectionThreshold(eq(HostDistance.REMOTE), eq(111));
	}

	@Test
	public void newLocalHostDistancePoolingOptionsReturnsLocalHostDistancePoolingOptionsFactoryBeanSettings() {
		poolingOptionsFactoryBean.setLocalCoreConnections(50);
		poolingOptionsFactoryBean.setLocalMaxConnections(500);
		poolingOptionsFactoryBean.setLocalMaxSimultaneousRequests(1000);
		poolingOptionsFactoryBean.setLocalMinSimultaneousRequests(100);
		poolingOptionsFactoryBean.setRemoteCoreConnections(20);
		poolingOptionsFactoryBean.setRemoteMaxConnections(200);
		poolingOptionsFactoryBean.setRemoteMaxSimultaneousRequests(400);
		poolingOptionsFactoryBean.setRemoteMinSimultaneousRequests(40);

		PoolingOptionsFactoryBean.HostDistancePoolingOptions poolingOptions =
			poolingOptionsFactoryBean.newLocalHostDistancePoolingOptions();

		assertThat(poolingOptions.getHostDistance(), is(equalTo(HostDistance.LOCAL)));
		assertThat(poolingOptions.getCoreConnectionsPerHost(), is(equalTo(50)));
		assertThat(poolingOptions.getMaxConnectionsPerHost(), is(equalTo(500)));
		assertThat(poolingOptions.getMaxRequestsPerConnection(), is(equalTo(1000)));
		assertThat(poolingOptions.getNewConnectionThreshold(), is(equalTo(100)));
	}

	@Test
	public void newLocalHostDistancePoolingOptionsReturnsRemoteHostDistancePoolingOptionsFactoryBeanSettings() {
		poolingOptionsFactoryBean.setLocalCoreConnections(50);
		poolingOptionsFactoryBean.setLocalMaxConnections(500);
		poolingOptionsFactoryBean.setLocalMaxSimultaneousRequests(1000);
		poolingOptionsFactoryBean.setLocalMinSimultaneousRequests(100);
		poolingOptionsFactoryBean.setRemoteCoreConnections(20);
		poolingOptionsFactoryBean.setRemoteMaxConnections(200);
		poolingOptionsFactoryBean.setRemoteMaxSimultaneousRequests(400);
		poolingOptionsFactoryBean.setRemoteMinSimultaneousRequests(40);

		PoolingOptionsFactoryBean.HostDistancePoolingOptions poolingOptions =
			poolingOptionsFactoryBean.newRemoteHostDistancePoolingOptions();

		assertThat(poolingOptions.getHostDistance(), is(equalTo(HostDistance.REMOTE)));
		assertThat(poolingOptions.getCoreConnectionsPerHost(), is(equalTo(20)));
		assertThat(poolingOptions.getMaxConnectionsPerHost(), is(equalTo(200)));
		assertThat(poolingOptions.getMaxRequestsPerConnection(), is(equalTo(400)));
		assertThat(poolingOptions.getNewConnectionThreshold(), is(equalTo(40)));
	}

	@Test
	public void configureLocalHostDistancePoolingOptionsCallsConfigureWithExpectedInstance() {
		final PoolingOptionsFactoryBean.HostDistancePoolingOptions mockHostDistancePoolingOptions = mock(
			PoolingOptionsFactoryBean.HostDistancePoolingOptions.class);

		when(mockHostDistancePoolingOptions.configure(any(PoolingOptions.class))).thenAnswer(
			new Answer<PoolingOptions>() {
				@Override
				public PoolingOptions answer(InvocationOnMock invocationOnMock) throws Throwable {
					return invocationOnMock.getArgumentAt(0, PoolingOptions.class);
				}
			}
		);

		poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override protected HostDistancePoolingOptions newLocalHostDistancePoolingOptions() {
				return mockHostDistancePoolingOptions;
			}
		};

		assertThat(poolingOptionsFactoryBean.configureLocalHostDistancePoolingOptions(poolingOptionsSpy),
			is(sameInstance(poolingOptionsSpy)));

		verify(mockHostDistancePoolingOptions, times(1)).configure(same(poolingOptionsSpy));
	}

	@Test
	public void configureRemoteHostDistancePoolingOptionsCallsConfigureWithExpectedInstance() {
		final PoolingOptionsFactoryBean.HostDistancePoolingOptions mockHostDistancePoolingOptions = mock(
			PoolingOptionsFactoryBean.HostDistancePoolingOptions.class);

		when(mockHostDistancePoolingOptions.configure(any(PoolingOptions.class))).thenAnswer(
			new Answer<PoolingOptions>() {
				@Override
				public PoolingOptions answer(InvocationOnMock invocationOnMock) throws Throwable {
					return invocationOnMock.getArgumentAt(0, PoolingOptions.class);
				}
			}
		);

		poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override protected HostDistancePoolingOptions newRemoteHostDistancePoolingOptions() {
				return mockHostDistancePoolingOptions;
			}
		};

		assertThat(poolingOptionsFactoryBean.configureRemoteHostDistancePoolingOptions(poolingOptionsSpy),
			is(sameInstance(poolingOptionsSpy)));

		verify(mockHostDistancePoolingOptions, times(1)).configure(same(poolingOptionsSpy));
	}
}
