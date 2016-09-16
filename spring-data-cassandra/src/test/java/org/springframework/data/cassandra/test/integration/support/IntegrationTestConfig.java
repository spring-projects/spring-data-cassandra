/*
 * Copyright 2013-2016 the original author or authors
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

import static org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification.*;

import java.util.Arrays;
import java.util.List;

import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.support.RandomKeySpaceName;
import org.springframework.cassandra.test.integration.support.CassandraConnectionProperties;
import org.springframework.cassandra.test.integration.support.FastShutdownNettyOptions;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.java.AbstractReactiveCassandraConfiguration;

import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.QueryOptions;

/**
 * Setup any spring configuration for unit tests
 *
 * @author David Webb
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@Configuration
public class IntegrationTestConfig extends AbstractReactiveCassandraConfiguration {

	public static final CassandraConnectionProperties PROPS = new CassandraConnectionProperties();
	public static final int PORT = PROPS.getCassandraPort();

	public String keyspaceName = RandomKeySpaceName.create();

	@Override
	protected int getPort() {
		return PORT;
	}

	@Override
	public SchemaAction getSchemaAction() {
		return SchemaAction.RECREATE;
	}

	@Override
	protected String getKeyspaceName() {
		return keyspaceName;
	}

	@Override
	protected NettyOptions getNettyOptions() {
		return FastShutdownNettyOptions.INSTANCE;
	}

	@Override
	protected QueryOptions getQueryOptions() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setRefreshSchemaIntervalMillis(0);
		return queryOptions;
	}

	@Override
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return Arrays.asList(createKeyspace().name(getKeyspaceName()).withSimpleReplication());
	}

	@Override
	protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return Arrays.asList(DropKeyspaceSpecification.dropKeyspace().name(getKeyspaceName()));
	}
}
