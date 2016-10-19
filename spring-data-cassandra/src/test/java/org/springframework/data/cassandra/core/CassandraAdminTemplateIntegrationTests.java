/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.test.integration.simpletons.Book;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

/**
 * Integration tests for {@link CassandraAdminTemplate}.
 *
 * @author Mark Paluch
 */
public class CassandraAdminTemplateIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private CassandraAdminTemplate cassandraAdminTemplate;

	@Before
	public void before() {

		cassandraAdminTemplate = new CassandraAdminTemplate(session, new MappingCassandraConverter());

		KeyspaceMetadata keyspace = getKeyspaceMetadata();
		Collection<TableMetadata> tables = keyspace.getTables();
		for (TableMetadata table : tables) {
			cassandraAdminTemplate.getCqlOperations()
					.execute(DropTableCqlGenerator.toCql(DropTableSpecification.dropTable(table.getName())));
		}
	}

	private KeyspaceMetadata getKeyspaceMetadata() {
		Metadata metadata = getSession().getCluster().getMetadata();
		return metadata.getKeyspace(getSession().getLoggedKeyspace());
	}

	/**
	 * @see DATACASS-173
	 */
	@Test
	public void testCreateTables() throws Exception {

		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);

		cassandraAdminTemplate.createTable(true, CqlIdentifier.cqlId("book"), Book.class, null);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);

		cassandraAdminTemplate.createTable(true, CqlIdentifier.cqlId("book"), Book.class, null);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);
	}

	@Test
	public void testDropTable() throws Exception {

		cassandraAdminTemplate.createTable(true, CqlIdentifier.cqlId("book"), Book.class, null);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);

		cassandraAdminTemplate.dropTable(Book.class);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);

		cassandraAdminTemplate.createTable(true, CqlIdentifier.cqlId("book"), Book.class, null);
		cassandraAdminTemplate.dropTable(CqlIdentifier.cqlId("book"));

		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);
	}
}
