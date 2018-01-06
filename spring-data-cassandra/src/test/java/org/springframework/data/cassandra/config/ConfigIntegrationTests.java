/*
 * Copyright 2017-2018 the original author or authors.
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.support.AbstractTestJavaConfig;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.datastax.driver.core.Session;

/**
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = ConfigIntegrationTests.Config.class)
public class ConfigIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	@Configuration
	static class Config extends AbstractTestJavaConfig {

		@Override
		protected String getKeyspaceName() {
			return null;
		}
	}

	@Autowired Session session;

	@Test
	public void test() {

		session.execute("DROP KEYSPACE IF EXISTS ConfigTest");

		session.execute("CREATE KEYSPACE ConfigTest " + "WITH "
				+ "REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

		session.execute("USE ConfigTest");
	}

}
