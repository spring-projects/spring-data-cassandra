/*
 * Copyright 2014-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.cdi;

import java.net.InetSocketAddress;
import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaCreator;
import org.springframework.data.cassandra.core.CassandraPersistentEntitySchemaDropper;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.support.CassandraConnectionProperties;
import org.springframework.data.cassandra.support.RandomKeyspaceName;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * @author Mark Paluch
 */
class CassandraOperationsProducer {

	private static final String KEYSPACE_NAME = RandomKeyspaceName.create();

	@Produces
	@Singleton
	public CqlSession createSession() throws Exception {
		CassandraConnectionProperties properties = new CassandraConnectionProperties();

		return CqlSession.builder().addContactPoint(
				InetSocketAddress.createUnresolved(properties.getCassandraHost(), properties.getCassandraPort())).build();
	}

	@Produces
	@ApplicationScoped
	public CassandraOperations createCassandraOperations(CqlSession session) throws Exception {

		CassandraMappingContext mappingContext = new CassandraMappingContext();
		mappingContext.setUserTypeResolver(new SimpleUserTypeResolver(session, CqlIdentifier.fromCql(KEYSPACE_NAME)));
		mappingContext.setInitialEntitySet(Collections.singleton(User.class));
		mappingContext.afterPropertiesSet();

		MappingCassandraConverter cassandraConverter = new MappingCassandraConverter(mappingContext);

		CassandraAdminTemplate cassandraTemplate = new CassandraAdminTemplate(session, cassandraConverter);

		CreateKeyspaceSpecification createKeyspaceSpecification = CreateKeyspaceSpecification.createKeyspace(KEYSPACE_NAME)
				.ifNotExists();
		cassandraTemplate.getCqlOperations().execute(CreateKeyspaceCqlGenerator.toCql(createKeyspaceSpecification));
		cassandraTemplate.getCqlOperations().execute("USE " + KEYSPACE_NAME);

		CassandraPersistentEntitySchemaDropper schemaDropper = new CassandraPersistentEntitySchemaDropper(mappingContext,
				cassandraTemplate);
		schemaDropper.dropTables(false);
		schemaDropper.dropUserTypes(false);

		CassandraPersistentEntitySchemaCreator schemaCreator = new CassandraPersistentEntitySchemaCreator(mappingContext,
				cassandraTemplate);
		schemaCreator.createUserTypes(false);
		schemaCreator.createTables(false);

		for (CassandraPersistentEntity<?> entity : cassandraTemplate.getConverter().getMappingContext()
				.getTableEntities()) {
			cassandraTemplate.truncate(entity.getType());
		}

		return cassandraTemplate;
	}

	@OtherQualifier
	@UserDB
	@Produces
	@ApplicationScoped
	public CassandraOperations createQualifiedCassandraOperations(CassandraOperations cassandraOperations) {
		return cassandraOperations;
	}

	public void close(@Disposes CassandraOperations cassandraOperations) {

		cassandraOperations.getCqlOperations()
				.execute(DropKeyspaceCqlGenerator.toCql(DropKeyspaceSpecification.dropKeyspace(KEYSPACE_NAME)));
	}

	public void close(@Disposes CqlSession cqlSession) {
		cqlSession.close();
	}
}
