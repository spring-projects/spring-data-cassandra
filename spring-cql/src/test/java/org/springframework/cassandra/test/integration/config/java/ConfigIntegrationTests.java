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
package org.springframework.cassandra.test.integration.config.java;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Matthew T. Adams
 */
@ContextConfiguration(classes = Config.class)
public class ConfigIntegrationTests extends AbstractIntegrationTest {

	@Test
	public void test() {

		session.execute("DROP KEYSPACE IF EXISTS ConfigTest");

		session.execute("CREATE KEYSPACE ConfigTest " + "WITH "
				+ "REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

		session.execute("USE ConfigTest");
	}
}
