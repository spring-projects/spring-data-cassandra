/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.generator.CreateIndexCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.SASI;
import org.springframework.data.cassandra.core.mapping.SASI.StandardAnalyzed;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.util.Version;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Integration tests usin {@link CassandraMappingContext} and {@link CreateIndexSpecification} to integratively verify
 * index creation.
 *
 * @author Mark Paluch
 */
class IndexCreationIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private CassandraMappingContext mappingContext = new CassandraMappingContext();
	private SchemaFactory schemaFactory = new SchemaFactory(new MappingCassandraConverter(mappingContext));
	private Version cassandraVersion;

	@BeforeEach
	void before() {

		cassandraVersion = CassandraVersion.get(session);

		assumeTrue(cassandraVersion.isGreaterThanOrEqualTo(Version.parse("3.4")));
	}

	@Test
	void shouldCreateSecondaryIndex() throws InterruptedException {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(WithSecondaryIndex.class);
		CreateTableSpecification createTable = schemaFactory.getCreateTableSpecificationFor(entity);
		List<CreateIndexSpecification> createIndexes = schemaFactory.getCreateIndexSpecificationsFor(entity);

		session.execute(CreateTableCqlGenerator.toCql(createTable));
		createIndexes.forEach(it -> session.execute(CreateIndexCqlGenerator.toCql(it)));

		Thread.sleep(500); // index creation is async so we do poor man's sync to await completion

		TableMetadata metadata = getMetadata(createTable.getName());

		assertThat(metadata.getIndexes().get(CqlIdentifier.fromCql("firstname_index"))).isNotNull();
		assertThat(metadata.getIndexes().get(CqlIdentifier.fromCql("withsecondaryindex_map_idx"))).isNotNull();
	}

	@Test
	void shouldCreateSasiIndex() throws InterruptedException {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(WithSasiIndex.class);
		CreateTableSpecification createTable = schemaFactory.getCreateTableSpecificationFor(entity);
		List<CreateIndexSpecification> createIndexes = schemaFactory.getCreateIndexSpecificationsFor(entity);

		session.execute(CreateTableCqlGenerator.toCql(createTable));
		createIndexes.forEach(it -> session.execute(CreateIndexCqlGenerator.toCql(it)));

		Thread.sleep(500); // index creation is async so we do poor man's sync to await completion

		TableMetadata metadata = getMetadata(createTable.getName());

		assertThat(metadata.getIndexes().get(CqlIdentifier.fromCql("withsasiindex_firstname_idx"))).isNotNull();
	}

	private TableMetadata getMetadata(CqlIdentifier tableName) {
		return session.refreshSchema().getKeyspace(session.getKeyspace().get()).flatMap(it -> it.getTable(tableName)).get();
	}

	private static class WithSecondaryIndex {

		@Id String id;

		@Indexed("firstname_index") String firstname;

		Map<String, @Indexed String> map;
	}

	private static class WithSasiIndex {

		@Id String id;

		@SASI @StandardAnalyzed String firstname;
	}
}
