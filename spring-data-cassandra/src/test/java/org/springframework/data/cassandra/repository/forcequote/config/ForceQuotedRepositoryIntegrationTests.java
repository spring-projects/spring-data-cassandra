/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.forcequote.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Matthew T. Adams
 */
@SpringJUnitConfig
public abstract class ForceQuotedRepositoryIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired CassandraOperations template;
	@Autowired ImplicitRepository implicitRepository;
	@Autowired ImplicitPropertiesRepository implicitPropertiesRepository;
	@Autowired ExplicitRepository explicitRepository;
	@Autowired ExplicitPropertiesRepository explicitPropertiesRepository;

	ForceQuotedRepositoryTests tests;

	@BeforeEach
	void before() {
		tests = new ForceQuotedRepositoryTests();
		tests.implicitRepository = implicitRepository;
		tests.implicitPropertiesRepository = implicitPropertiesRepository;
		tests.explicitRepository = explicitRepository;
		tests.explicitPropertiesRepository = explicitPropertiesRepository;
		tests.cassandraTemplate = template;

		tests.before();
	}

	@Test
	void testImplicit() {
		tests.testImplicit();
	}

	/**
	 * Not a @Test -- used by subclasses!
	 */
	void testExplicit(String tableName) {
		tests.testExplicit(tableName);
	}

	@Test
	void testImplicitProperties() {
		tests.testImplicitProperties();
	}

	/**
	 * Not a @Test -- used by subclasses!
	 *
	 * @see ForceQuotedRepositoryJavaConfigIntegrationTests#testExplicitPropertiesWithJavaValues()
	 * @see ForceQuotedRepositoryXmlConfigIntegrationTests#testExplicitPropertiesWithXmlValues()
	 */
	void testExplicitProperties(String stringValueColumnName, String primaryKeyColumnName) {
		tests.testExplicitProperties(stringValueColumnName, primaryKeyColumnName);
	}
}
