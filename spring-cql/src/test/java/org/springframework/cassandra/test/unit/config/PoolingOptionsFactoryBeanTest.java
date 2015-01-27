/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.unit.config;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.cassandra.config.PoolingOptionsFactoryBean;

/**
 * Pooling Options Factory Bean Test. https://jira.spring.io/browse/DATACASS-176
 * 
 * @author Sumit Kumar
 * @author David Webb
 */
public class PoolingOptionsFactoryBeanTest {

	private static final int REMOTE_MIN_SIMULTANEOUS_REQUESTS = 111;
	private static final int REMOTE_MAX_SIMULTANEOUS_REQUESTS = 127;
	private static final int REMOTE_CORE_CONNECTIONS = 110;
	private static final int REMOTE_MAX_CONNECTIONS = 210;
	private static final int LOCAL_MIN_SIMULTANEOUS_REQUESTS = 97;
	private static final int LOCAL_MAX_SIMULTANEOUS_REQUESTS = 99;
	private static final int LOCAL_CORE_CONNECTIONS = 100;
	private static final int LOCAL_MAX_CONNECTIONS = 200;

	/**
	 * The max values should be set before setting core values. Otherwise the core values will be compared with the
	 * default max values which is 8. Same for other min-max properties pairs. This test checks the same.
	 * 
	 * @throws Exception Any unhandled scenarios will result in a test failure.
	 */
	@Test
	public void testAfterPropertiesSet() throws Exception {

		PoolingOptionsFactoryBean factoryBean = new PoolingOptionsFactoryBean();
		factoryBean.setLocalMaxConnections(LOCAL_MAX_CONNECTIONS);
		factoryBean.setLocalCoreConnections(LOCAL_CORE_CONNECTIONS);
		factoryBean.setLocalMaxSimultaneousRequests(LOCAL_MAX_SIMULTANEOUS_REQUESTS);
		factoryBean.setLocalMinSimultaneousRequests(LOCAL_MIN_SIMULTANEOUS_REQUESTS);
		factoryBean.setRemoteMaxConnections(REMOTE_MAX_CONNECTIONS);
		factoryBean.setRemoteCoreConnections(REMOTE_CORE_CONNECTIONS);
		factoryBean.setRemoteMaxSimultaneousRequests(REMOTE_MAX_SIMULTANEOUS_REQUESTS);
		factoryBean.setRemoteMinSimultaneousRequests(REMOTE_MIN_SIMULTANEOUS_REQUESTS);
		factoryBean.afterPropertiesSet();

		Assert.assertEquals(factoryBean.getLocalMaxConnections().intValue(), LOCAL_MAX_CONNECTIONS);
		Assert.assertEquals(factoryBean.getLocalCoreConnections().intValue(), LOCAL_CORE_CONNECTIONS);
		Assert.assertEquals(factoryBean.getLocalMaxSimultaneousRequests().intValue(), LOCAL_MAX_SIMULTANEOUS_REQUESTS);
		Assert.assertEquals(factoryBean.getLocalMinSimultaneousRequests().intValue(), LOCAL_MIN_SIMULTANEOUS_REQUESTS);
		Assert.assertEquals(factoryBean.getRemoteMaxConnections().intValue(), REMOTE_MAX_CONNECTIONS);
		Assert.assertEquals(factoryBean.getRemoteCoreConnections().intValue(), REMOTE_CORE_CONNECTIONS);
		Assert.assertEquals(factoryBean.getRemoteMaxSimultaneousRequests().intValue(), REMOTE_MAX_SIMULTANEOUS_REQUESTS);
		Assert.assertEquals(factoryBean.getRemoteMinSimultaneousRequests().intValue(), REMOTE_MIN_SIMULTANEOUS_REQUESTS);

	}

}
