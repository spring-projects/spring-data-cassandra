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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceAttributes;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption;
import org.springframework.data.cassandra.support.AbstractTestJavaConfig;
import org.springframework.data.cassandra.support.KeyspaceTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.datastax.driver.core.Session;

/**
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = KeyspaceCreatingJavaConfigIntegrationTests.KeyspaceCreatingJavaConfig.class)
public class KeyspaceCreatingJavaConfigIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	@Autowired Session session;

	@Test
	public void test() {

		assertThat(session).isNotNull();
		KeyspaceTestUtils.assertKeyspaceExists(KeyspaceCreatingJavaConfig.KEYSPACE_NAME, session);

		session.execute("DROP KEYSPACE " + KeyspaceCreatingJavaConfig.KEYSPACE_NAME + ";");
	}

	/**
	 * @author Matthew T. Adams
	 * @author Mark Paluch
	 */
	@Configuration
	static class KeyspaceCreatingJavaConfig extends AbstractTestJavaConfig {

		public static final String KEYSPACE_NAME = "foo";

		@Override
		protected String getKeyspaceName() {
			return KEYSPACE_NAME;
		}

		@Override
		protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
			ArrayList<CreateKeyspaceSpecification> list = new ArrayList<>();

			CreateKeyspaceSpecification specification = CreateKeyspaceSpecification.createKeyspace(getKeyspaceName());
			specification.with(KeyspaceOption.REPLICATION, KeyspaceAttributes.newSimpleReplication(1L));

			list.add(specification);
			return list;
		}
	}
}
