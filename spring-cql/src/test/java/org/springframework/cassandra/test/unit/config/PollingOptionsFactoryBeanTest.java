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

import junit.framework.Assert;
import org.junit.Test;
import org.springframework.cassandra.config.PoolingOptionsFactoryBean;

/**
 * Pooling Options Factory Bean Test.
 * https://jira.spring.io/browse/DATACASS-176
 * 
 * @author Sumit Kumar
 */
public class PollingOptionsFactoryBeanTest {
	
	/**
	 * The max values should be set before setting core values.
	 * Otherwise the core values will be compared with the default max values which is 8.
	 * Same for other min-max properties pairs. This test checks the same.
	 */
	@Test
	public void testAfterPropertiesSet() {
		boolean gotException = false;
		PoolingOptionsFactoryBean factoryBean = new PoolingOptionsFactoryBean();
		factoryBean.setLocalMaxConnections(200);
		factoryBean.setLocalCoreConnections(100);
		factoryBean.setLocalMaxSimultaneousRequests(128);
		factoryBean.setLocalMinSimultaneousRequests(101);
		factoryBean.setRemoteMaxConnections(200);
		factoryBean.setRemoteCoreConnections(100);
		factoryBean.setRemoteMaxSimultaneousRequests(128);
		factoryBean.setRemoteMinSimultaneousRequests(101);
		try {
			factoryBean.afterPropertiesSet();
		} catch (Exception e) {
			e.printStackTrace();
			gotException = true;
		}
		Assert.assertFalse(gotException);
	}

}
