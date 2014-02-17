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

import static org.springframework.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator.toCql;
import static org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification.createKeyspace;

import org.springframework.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.cassandra.config.java.AbstractSessionConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

@Configuration
public abstract class AbstractKeyspaceCreatingConfiguration extends AbstractSessionConfiguration {

	@Override
	public CassandraSessionFactoryBean session() throws Exception {

		createKeyspaceIfNecessary();

		return super.session();
	}

	protected void createKeyspaceIfNecessary() throws Exception {
		String keyspace = getKeyspaceName();
		if (!StringUtils.hasText(keyspace)) {
			return;
		}

		Session system = cluster().getObject().connect();
		KeyspaceMetadata kmd = system.getCluster().getMetadata().getKeyspace(keyspace);
		if (kmd != null) {
			return;
		}

		system.execute(toCql(createKeyspace().name(keyspace).withSimpleReplication()));
		system.shutdown();
	}
}
