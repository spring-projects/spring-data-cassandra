/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.generator;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

/**
 * Test CREATE TABLE for all Options and assert against C* TableMetaData
 *
 * @author David Webb
 * @author Oliver Gierke
 */
public class TableOptionsIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private static final Logger log = LoggerFactory.getLogger(TableOptionsIntegrationTests.class);

	@Test
	public void test() {

		CreateTableCqlGeneratorUnitTests.MultipleOptionsTest optionsTest = new CreateTableCqlGeneratorUnitTests.MultipleOptionsTest();

		optionsTest.prepare();

		log.info(optionsTest.cql);

		session.execute(optionsTest.cql);

		CqlTableSpecificationAssertions.assertTable(optionsTest.specification, keyspace, session);
	}
}
