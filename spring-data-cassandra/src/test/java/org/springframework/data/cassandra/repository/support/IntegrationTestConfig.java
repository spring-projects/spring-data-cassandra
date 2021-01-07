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
package org.springframework.data.cassandra.repository.support;

import static org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification.*;

import java.util.Collections;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.config.AbstractReactiveCassandraConfiguration;
import org.springframework.data.cassandra.config.CqlSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.support.CassandraConnectionProperties;
import org.springframework.data.cassandra.support.RandomKeyspaceName;

/**
 * Setup any spring configuration for unit tests
 *
 * @author David Webb
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@Configuration
public class IntegrationTestConfig extends AbstractReactiveCassandraConfiguration {

	private static final CassandraConnectionProperties PROPS = new CassandraConnectionProperties();
	private static final int PORT = PROPS.getCassandraPort();

	private String keyspaceName = RandomKeyspaceName.create();

	@Override
	protected int getPort() {
		return PORT;
	}

	@Bean
	@Override
	public CqlSessionFactoryBean cassandraSession() {

		SharedCqlSessionFactoryBean bean = new SharedCqlSessionFactoryBean();

		bean.setContactPoints(getContactPoints());
		bean.setPort(getPort());

		bean.setKeyspaceCreations(getKeyspaceCreations());
		bean.setKeyspaceDrops(getKeyspaceDrops());

		bean.setKeyspaceName(getKeyspaceName());
		bean.setKeyspaceStartupScripts(getStartupScripts());
		bean.setKeyspaceShutdownScripts(getShutdownScripts());

		return bean;
	}

	@Bean(destroyMethod = "")
	@Override
	public ReactiveSession reactiveCassandraSession() {
		return super.reactiveCassandraSession();
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
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return Collections.singletonList(createKeyspace(getKeyspaceName()).withSimpleReplication());
	}

	@Override
	protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return Collections.singletonList(DropKeyspaceSpecification.dropKeyspace(getKeyspaceName()));
	}
}
