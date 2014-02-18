/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;

public class AbstractSpringDataEmbeddedCassandraIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	static {
		SpringDataCassandraBuildProperties props = new SpringDataCassandraBuildProperties();
		CASSANDRA_NATIVE_PORT = props.getCassandraPort();
	}

	public Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	public CassandraOperations template;

	public void deleteAllEntities() {

		for (CassandraPersistentEntity<?> entity : template.getConverter().getMappingContext().getPersistentEntities()) {
			template.truncate(entity.getTableName());
		}
	}
}
