/*
 * Copyright 2014-2016 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repository.cdi;

import java.util.HashMap;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.test.integration.repository.User;

/**
 * @author Mark Paluch
 */
@ApplicationScoped
class CassandraOperationsProducer {

	@Produces
	public CassandraOperations createCassandraOperations() throws Exception {
		String keySpace = AbstractEmbeddedCassandraIntegrationTest.randomKeyspaceName();

		MappingCassandraConverter cassandraConverter = new MappingCassandraConverter();
		CassandraAdminTemplate cassandraTemplate = new CassandraAdminTemplate(AbstractEmbeddedCassandraIntegrationTest
				.cluster().connect(), cassandraConverter);

		CreateKeyspaceSpecification createKeyspaceSpecification = new CreateKeyspaceSpecification(keySpace).ifNotExists();
		cassandraTemplate.execute(createKeyspaceSpecification);
		cassandraTemplate.execute("USE " + keySpace);

		cassandraTemplate.createTable(true, CqlIdentifier.cqlId("users"), User.class, new HashMap<String, Object>());

		for (CassandraPersistentEntity<?> entity : cassandraTemplate.getConverter().getMappingContext()
				.getPersistentEntities()) {
			cassandraTemplate.truncate(entity.getTableName());
		}

		return cassandraTemplate;
	}

	@OtherQualifier
 	@UserDB
    @Produces
	@ApplicationScoped
	public CassandraOperations createQualifiedCassandraOperations(CassandraOperations cassandraOperations){
		return cassandraOperations;
	}

	public void close(@Disposes CassandraOperations cassandraOperations) {
		cassandraOperations.getSession().close();
	}

	@Produces
	public Set<Service> producerToSatisfyGuavaDependenciesWhenTesting() {
		return Sets.newHashSet();
	}

}
