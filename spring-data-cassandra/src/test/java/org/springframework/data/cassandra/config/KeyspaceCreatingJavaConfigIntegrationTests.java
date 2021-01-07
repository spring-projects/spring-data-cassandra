/*
 * Copyright 2017-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceAttributes;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption;
import org.springframework.data.cassandra.support.AbstractTestJavaConfig;
import org.springframework.data.cassandra.support.KeyspaceTestUtils;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@SpringJUnitConfig(classes = KeyspaceCreatingJavaConfigIntegrationTests.KeyspaceCreatingJavaConfig.class)
class KeyspaceCreatingJavaConfigIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	@Autowired CqlSession session;

	@Test
	void test() {

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

		private static final String KEYSPACE_NAME = "foo";

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
