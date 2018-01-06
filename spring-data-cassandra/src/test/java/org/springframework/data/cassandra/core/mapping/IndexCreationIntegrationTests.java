/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.generator.CreateIndexCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.mapping.SASI.StandardAnalyzed;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.util.Version;

import com.datastax.driver.core.TableMetadata;

/**
 * Integration tests usin {@link CassandraMappingContext} and {@link CreateIndexSpecification} to integratively verify
 * index creation.
 *
 * @author Mark Paluch
 */
public class IndexCreationIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	private CassandraMappingContext mappingContext = new CassandraMappingContext();
	private Version cassandraVersion;

	@Before
	public void before() {

		cassandraVersion = CassandraVersion.get(session);

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(Version.parse("3.4")));
	}

	@Test
	public void shouldCreateSecondaryIndex() throws InterruptedException {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(WithSecondaryIndex.class);
		CreateTableSpecification createTable = mappingContext.getCreateTableSpecificationFor(entity);
		List<CreateIndexSpecification> createIndexes = mappingContext.getCreateIndexSpecificationsFor(entity);

		session.execute(CreateTableCqlGenerator.toCql(createTable));
		createIndexes.forEach(it -> session.execute(CreateIndexCqlGenerator.toCql(it)));

		Thread.sleep(500); // index creation is async so we do poor man's sync to await completion

		TableMetadata metadata = getMetadata(createTable.getName().toCql());

		assertThat(metadata.getIndex("firstname_index")).isNotNull();
		assertThat(metadata.getIndex("withsecondaryindex_map_idx")).isNotNull();
	}

	@Test
	public void shouldCreateSasiIndex() throws InterruptedException {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(WithSasiIndex.class);
		CreateTableSpecification createTable = mappingContext.getCreateTableSpecificationFor(entity);
		List<CreateIndexSpecification> createIndexes = mappingContext.getCreateIndexSpecificationsFor(entity);

		session.execute(CreateTableCqlGenerator.toCql(createTable));
		createIndexes.forEach(it -> session.execute(CreateIndexCqlGenerator.toCql(it)));

		Thread.sleep(500); // index creation is async so we do poor man's sync to await completion

		TableMetadata metadata = getMetadata(createTable.getName().toCql());

		assertThat(metadata.getIndex("withsasiindex_firstname_idx")).isNotNull();
	}

	private TableMetadata getMetadata(String tableName) {
		return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable(tableName);
	}

	static class WithSecondaryIndex {

		@Id String id;

		@Indexed("firstname_index") String firstname;

		Map<String, @Indexed String> map;
	}

	static class WithSasiIndex {

		@Id String id;

		@SASI @StandardAnalyzed String firstname;
	}
}
