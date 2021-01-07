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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification;
import org.springframework.data.cassandra.domain.User;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Integration tests for {@link CassandraAdminTemplate}.
 *
 * @author Mark Paluch
 */
class CassandraAdminTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraAdminTemplate cassandraAdminTemplate;

	@BeforeEach
	void before() {

		cassandraAdminTemplate = new CassandraAdminTemplate(session, new MappingCassandraConverter());

		KeyspaceMetadata keyspace = getKeyspaceMetadata();
		Collection<TableMetadata> tables = keyspace.getTables().values();
		for (TableMetadata table : tables) {
			cassandraAdminTemplate.getCqlOperations()
					.execute(DropTableCqlGenerator.toCql(DropTableSpecification.dropTable(table.getName())));
		}
	}

	private KeyspaceMetadata getKeyspaceMetadata() {
		Metadata metadata = getSession().getMetadata();
		return getSession().getKeyspace().flatMap(metadata::getKeyspace).get();
	}

	@Test // DATACASS-173
	void testCreateTables() {

		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);

		cassandraAdminTemplate.createTable(true, CqlIdentifier.fromCql("users"), User.class, null);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);

		cassandraAdminTemplate.createTable(true, CqlIdentifier.fromCql("users"), User.class, null);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);
	}

	@Test
	void testDropTable() {

		cassandraAdminTemplate.createTable(true, CqlIdentifier.fromCql("users"), User.class, null);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);

		cassandraAdminTemplate.dropTable(User.class);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);

		cassandraAdminTemplate.createTable(true, CqlIdentifier.fromCql("users"), User.class, null);
		cassandraAdminTemplate.dropTable(CqlIdentifier.fromCql("users"));

		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);
	}
}
