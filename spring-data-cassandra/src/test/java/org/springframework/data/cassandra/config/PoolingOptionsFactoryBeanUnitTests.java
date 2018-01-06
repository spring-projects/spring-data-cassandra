/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.*;
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;

/**
 * Unit tests for {@link PoolingOptionsFactoryBean}.
 *
 * @author Sumit Kumar
 * @author David Webb
 * @author John Blum
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class PoolingOptionsFactoryBeanUnitTests {

	@Mock Executor mockExecutor;
	@Spy PoolingOptions poolingOptionsSpy;

	private PoolingOptionsFactoryBean poolingOptionsFactoryBean;

	@Before
	public void setup() {
		poolingOptionsFactoryBean = new PoolingOptionsFactoryBean();
	}

	@Test // DATACASS-176, DATACASS-298
	public void getObjectReturnsNullWhenNotInitialized() throws Exception {
		assertThat(poolingOptionsFactoryBean.getObject()).isNull();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void getObjectTypeReturnsPoolingOptionsClassWhenNotInitialized() {
		assertThat(poolingOptionsFactoryBean.getObjectType()).isEqualTo(PoolingOptions.class);
	}

	@Test
	public void isSingletonIsTrue() {
		assertThat(poolingOptionsFactoryBean.isSingleton()).isTrue();
	}

	@Test // DATACASS-298, DATACASS-344
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

		assertThat(poolingOptionsFactoryBean.getHeartbeatIntervalSeconds()).isEqualTo(15);
		assertThat(poolingOptionsFactoryBean.getIdleTimeoutSeconds()).isEqualTo(120);
		assertThat(poolingOptionsFactoryBean.getInitializationExecutor()).isEqualTo(mockExecutor);
		assertThat(poolingOptionsFactoryBean.getLocalCoreConnections()).isEqualTo(50);
		assertThat(poolingOptionsFactoryBean.getLocalMaxConnections()).isEqualTo(1000);
		assertThat(poolingOptionsFactoryBean.getLocalMaxSimultaneousRequests()).isEqualTo(200);
		assertThat(poolingOptionsFactoryBean.getLocalMinSimultaneousRequests()).isEqualTo(100);
		assertThat(poolingOptionsFactoryBean.getPoolTimeoutMilliseconds()).isEqualTo(300);
		assertThat(poolingOptionsFactoryBean.getRemoteCoreConnections()).isEqualTo(25);
		assertThat(poolingOptionsFactoryBean.getRemoteMaxConnections()).isEqualTo(250);
		assertThat(poolingOptionsFactoryBean.getRemoteMaxSimultaneousRequests()).isEqualTo(100);
		assertThat(poolingOptionsFactoryBean.getRemoteMinSimultaneousRequests()).isEqualTo(50);
	}

	@Test // DATACASS-298
	public void afterPropertiesSetInitializesLocalPoolingOptions() throws Exception {

		PoolingOptionsFactoryBean poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override
			PoolingOptions newPoolingOptions() {
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

		assertThat(poolingOptionsFactoryBean.getObject()).isNull();

		poolingOptionsFactoryBean.afterPropertiesSet();

		assertThat(poolingOptionsFactoryBean.getObject()).isSameAs(poolingOptionsSpy);
		assertThat(poolingOptionsFactoryBean.getObjectType()).isEqualTo(poolingOptionsSpy.getClass());

		verify(poolingOptionsSpy).setHeartbeatIntervalSeconds(eq(60));
		verify(poolingOptionsSpy).setIdleTimeoutSeconds(eq(300));
		verify(poolingOptionsSpy).setInitializationExecutor(eq(mockExecutor));
		verify(poolingOptionsSpy).setPoolTimeoutMillis(eq(180));
		verify(poolingOptionsSpy).setCoreConnectionsPerHost(eq(HostDistance.LOCAL), eq(10));
		verify(poolingOptionsSpy).setMaxConnectionsPerHost(eq(HostDistance.LOCAL), eq(100));
		verify(poolingOptionsSpy).setMaxRequestsPerConnection(eq(HostDistance.LOCAL), eq(50));
		verify(poolingOptionsSpy).setNewConnectionThreshold(eq(HostDistance.LOCAL), eq(5));
		verify(poolingOptionsSpy, never()).setCoreConnectionsPerHost(eq(HostDistance.REMOTE), anyInt());
		verify(poolingOptionsSpy, never()).setMaxConnectionsPerHost(eq(HostDistance.REMOTE), anyInt());
		verify(poolingOptionsSpy, never()).setMaxRequestsPerConnection(eq(HostDistance.REMOTE), anyInt());
		verify(poolingOptionsSpy, never()).setNewConnectionThreshold(eq(HostDistance.REMOTE), anyInt());
	}

	@Test // DATACASS-298
	public void afterPropertiesSetInitializesRemotePoolingOptions() throws Exception {

		PoolingOptionsFactoryBean poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override
			PoolingOptions newPoolingOptions() {
				poolingOptionsSpy.setNewConnectionThreshold(HostDistance.REMOTE, 10);
				return poolingOptionsSpy;
			}
		};

		poolingOptionsFactoryBean.setHeartbeatIntervalSeconds(33);
		poolingOptionsFactoryBean.setIdleTimeoutSeconds(112);
		poolingOptionsFactoryBean.setInitializationExecutor(mockExecutor);
		poolingOptionsFactoryBean.setPoolTimeoutMilliseconds(130);
		poolingOptionsFactoryBean.setRemoteCoreConnections(5);
		poolingOptionsFactoryBean.setRemoteMaxConnections(50);
		poolingOptionsFactoryBean.setRemoteMaxSimultaneousRequests(20);
		poolingOptionsFactoryBean.setRemoteMinSimultaneousRequests(5);

		assertThat(poolingOptionsFactoryBean.getObject()).isNull();

		poolingOptionsFactoryBean.afterPropertiesSet();

		assertThat(poolingOptionsFactoryBean.getObject()).isSameAs(poolingOptionsSpy);
		assertThat(poolingOptionsFactoryBean.getObjectType()).isEqualTo(poolingOptionsSpy.getClass());

		verify(poolingOptionsSpy).setHeartbeatIntervalSeconds(eq(33));
		verify(poolingOptionsSpy).setIdleTimeoutSeconds(eq(112));
		verify(poolingOptionsSpy).setInitializationExecutor(eq(mockExecutor));
		verify(poolingOptionsSpy).setPoolTimeoutMillis(eq(130));
		verify(poolingOptionsSpy).setCoreConnectionsPerHost(eq(HostDistance.REMOTE), eq(5));
		verify(poolingOptionsSpy).setMaxConnectionsPerHost(eq(HostDistance.REMOTE), eq(50));
		verify(poolingOptionsSpy).setMaxRequestsPerConnection(eq(HostDistance.REMOTE), eq(20));
		verify(poolingOptionsSpy, never()).setCoreConnectionsPerHost(eq(HostDistance.LOCAL), anyInt());
		verify(poolingOptionsSpy, never()).setMaxConnectionsPerHost(eq(HostDistance.LOCAL), anyInt());
		verify(poolingOptionsSpy, never()).setMaxRequestsPerConnection(eq(HostDistance.LOCAL), anyInt());
		verify(poolingOptionsSpy, never()).setNewConnectionThreshold(eq(HostDistance.LOCAL), anyInt());
		verify(poolingOptionsSpy, never()).setNewConnectionThreshold(eq(HostDistance.REMOTE), eq(5));
	}

	@Test // DATACASS-344
	public void afterPropertiesSetInitializesMaxQueueSize() throws Exception {

		Method setMaxQueueSize = ReflectionUtils.findMethod(PoolingOptions.class, "setMaxQueueSize", int.class);

		Method getMaxQueueSize = ReflectionUtils.findMethod(PoolingOptions.class, "getMaxQueueSize");

		assumeNotNull(setMaxQueueSize);

		PoolingOptionsFactoryBean poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override
			PoolingOptions newPoolingOptions() {
				return poolingOptionsSpy;
			}
		};

		poolingOptionsFactoryBean.setMaxQueueSize(1234);

		poolingOptionsFactoryBean.afterPropertiesSet();

		assertThat(poolingOptionsFactoryBean.getObject()).isSameAs(poolingOptionsSpy);
		assertThat(poolingOptionsFactoryBean.getObjectType()).isEqualTo(poolingOptionsSpy.getClass());
		assertThat(invokeMethod(getMaxQueueSize, poolingOptionsSpy)).isEqualTo(1234);
	}

	/**
	 * This particular test case is technically an integration test since it uses an actual instance of a DataStax Java
	 * driver class type... {@link PoolingOptions}! The max values should be set before setting core values. Otherwise the
	 * core values will be compared with the default max values which is 8. Same for other min-max properties pairs. This
	 * test checks the same.
	 */
	@Test // DATACASS-176
	public void afterPropertiesSetProperlySetsPoolingOptionsMaxBeforeMinProperties() throws Exception {

		poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override
			PoolingOptions newPoolingOptions() {
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

		assertThat(poolingOptionsFactoryBean.getObject()).isNull();

		poolingOptionsFactoryBean.afterPropertiesSet();

		PoolingOptions poolingOptions = poolingOptionsFactoryBean.getObject();

		assertThat(poolingOptions).isNotNull();
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.LOCAL)).isEqualTo(100);
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL)).isEqualTo(200);
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL)).isEqualTo(99);
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.LOCAL)).isEqualTo(97);
		assertThat(poolingOptions.getCoreConnectionsPerHost(HostDistance.REMOTE)).isEqualTo(110);
		assertThat(poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE)).isEqualTo(210);
		assertThat(poolingOptions.getMaxRequestsPerConnection(HostDistance.REMOTE)).isEqualTo(127);
		assertThat(poolingOptions.getNewConnectionThreshold(HostDistance.REMOTE)).isEqualTo(111);

		verify(poolingOptions).setMaxConnectionsPerHost(eq(HostDistance.LOCAL), eq(200));
		verify(poolingOptions).setCoreConnectionsPerHost(eq(HostDistance.LOCAL), eq(100));
		verify(poolingOptions).setMaxRequestsPerConnection(eq(HostDistance.LOCAL), eq(99));
		verify(poolingOptions).setNewConnectionThreshold(eq(HostDistance.LOCAL), eq(97));
		verify(poolingOptions).setMaxConnectionsPerHost(eq(HostDistance.REMOTE), eq(210));
		verify(poolingOptions).setCoreConnectionsPerHost(eq(HostDistance.REMOTE), eq(110));
		verify(poolingOptions).setMaxRequestsPerConnection(eq(HostDistance.REMOTE), eq(127));
		verify(poolingOptions).setNewConnectionThreshold(eq(HostDistance.REMOTE), eq(111));
	}

	@Test // DATACASS-176
	public void newLocalHostDistancePoolingOptionsReturnsLocalHostDistancePoolingOptionsFactoryBeanSettings() {

		poolingOptionsFactoryBean.setLocalCoreConnections(50);
		poolingOptionsFactoryBean.setLocalMaxConnections(500);
		poolingOptionsFactoryBean.setLocalMaxSimultaneousRequests(1000);
		poolingOptionsFactoryBean.setLocalMinSimultaneousRequests(100);
		poolingOptionsFactoryBean.setRemoteCoreConnections(20);
		poolingOptionsFactoryBean.setRemoteMaxConnections(200);
		poolingOptionsFactoryBean.setRemoteMaxSimultaneousRequests(400);
		poolingOptionsFactoryBean.setRemoteMinSimultaneousRequests(40);

		PoolingOptionsFactoryBean.HostDistancePoolingOptions poolingOptions = poolingOptionsFactoryBean
				.newLocalHostDistancePoolingOptions();

		assertThat(poolingOptions.getHostDistance()).isEqualTo(HostDistance.LOCAL);
		assertThat(poolingOptions.getCoreConnectionsPerHost()).isEqualTo(50);
		assertThat(poolingOptions.getMaxConnectionsPerHost()).isEqualTo(500);
		assertThat(poolingOptions.getMaxRequestsPerConnection()).isEqualTo(1000);
		assertThat(poolingOptions.getNewConnectionThreshold()).isEqualTo(100);
	}

	@Test // DATACASS-176
	public void newLocalHostDistancePoolingOptionsReturnsRemoteHostDistancePoolingOptionsFactoryBeanSettings() {

		poolingOptionsFactoryBean.setLocalCoreConnections(50);
		poolingOptionsFactoryBean.setLocalMaxConnections(500);
		poolingOptionsFactoryBean.setLocalMaxSimultaneousRequests(1000);
		poolingOptionsFactoryBean.setLocalMinSimultaneousRequests(100);
		poolingOptionsFactoryBean.setRemoteCoreConnections(20);
		poolingOptionsFactoryBean.setRemoteMaxConnections(200);
		poolingOptionsFactoryBean.setRemoteMaxSimultaneousRequests(400);
		poolingOptionsFactoryBean.setRemoteMinSimultaneousRequests(40);

		PoolingOptionsFactoryBean.HostDistancePoolingOptions poolingOptions = poolingOptionsFactoryBean
				.newRemoteHostDistancePoolingOptions();

		assertThat(poolingOptions.getHostDistance()).isEqualTo(HostDistance.REMOTE);
		assertThat(poolingOptions.getCoreConnectionsPerHost()).isEqualTo(20);
		assertThat(poolingOptions.getMaxConnectionsPerHost()).isEqualTo(200);
		assertThat(poolingOptions.getMaxRequestsPerConnection()).isEqualTo(400);
		assertThat(poolingOptions.getNewConnectionThreshold()).isEqualTo(40);
	}

	@Test // DATACASS-176
	public void configureLocalHostDistancePoolingOptionsCallsConfigureWithExpectedInstance() {

		final PoolingOptionsFactoryBean.HostDistancePoolingOptions mockHostDistancePoolingOptions = mock(
				PoolingOptionsFactoryBean.HostDistancePoolingOptions.class);

		when(mockHostDistancePoolingOptions.configure(any(PoolingOptions.class)))
				.thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

		poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override
			protected HostDistancePoolingOptions newLocalHostDistancePoolingOptions() {
				return mockHostDistancePoolingOptions;
			}
		};

		assertThat(poolingOptionsFactoryBean.configureLocalHostDistancePoolingOptions(poolingOptionsSpy))
				.isSameAs(poolingOptionsSpy);

		verify(mockHostDistancePoolingOptions).configure(same(poolingOptionsSpy));
	}

	@Test // DATACASS-176
	public void configureRemoteHostDistancePoolingOptionsCallsConfigureWithExpectedInstance() {

		final PoolingOptionsFactoryBean.HostDistancePoolingOptions mockHostDistancePoolingOptions = mock(
				PoolingOptionsFactoryBean.HostDistancePoolingOptions.class);

		when(mockHostDistancePoolingOptions.configure(any(PoolingOptions.class)))
				.thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

		poolingOptionsFactoryBean = new PoolingOptionsFactoryBean() {
			@Override
			protected HostDistancePoolingOptions newRemoteHostDistancePoolingOptions() {
				return mockHostDistancePoolingOptions;
			}
		};

		assertThat(poolingOptionsFactoryBean.configureRemoteHostDistancePoolingOptions(poolingOptionsSpy))
				.isSameAs(poolingOptionsSpy);

		verify(mockHostDistancePoolingOptions).configure(same(poolingOptionsSpy));
	}
}
