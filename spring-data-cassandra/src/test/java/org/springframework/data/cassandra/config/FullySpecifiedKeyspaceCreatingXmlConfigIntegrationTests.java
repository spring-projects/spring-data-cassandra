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
import org.springframework.data.cassandra.support.KeyspaceTestUtils;
import org.springframework.data.cassandra.test.util.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

/**
 * @author Matthew T. Adams
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class FullySpecifiedKeyspaceCreatingXmlConfigIntegrationTests extends AbstractEmbeddedCassandraIntegrationTest {

	@Autowired Session session;

	@Test
	public void test() {
		KeyspaceTestUtils.assertKeyspaceExists("full1", session);
		KeyspaceTestUtils.assertKeyspaceExists("full2", session);
		KeyspaceTestUtils.assertKeyspaceExists("script1", session);
		KeyspaceTestUtils.assertKeyspaceExists("script2", session);
	}
}
