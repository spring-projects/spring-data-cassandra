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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Integration tests for {@link CreateTableCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 */
class CreateTableCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	@BeforeEach
	void setUp() {

		session.execute("DROP TABLE IF EXISTS person;");
		session.execute("DROP TABLE IF EXISTS address;");
	}

	@Test // DATACASS-518
	void shouldGenerateSimpleTable() {

		CreateTableSpecification table = CreateTableSpecification.createTable("person") //
				.partitionKeyColumn("id", DataTypes.ASCII) //
				.clusteredKeyColumn("date_of_birth", DataTypes.DATE) //
				.column("name", DataTypes.ASCII);

		session.execute(CreateTableCqlGenerator.toCql(table));
	}

	@Test // DATACASS-518
	void shouldGenerateTableWithClusterKeyOrdering() {

		CreateTableSpecification table = CreateTableSpecification.createTable("person") //
				.partitionKeyColumn("id", DataTypes.ASCII) //
				.partitionKeyColumn("country", DataTypes.ASCII) //
				.clusteredKeyColumn("date_of_birth", DataTypes.DATE, Ordering.ASCENDING) //
				.clusteredKeyColumn("age", DataTypes.SMALLINT) //
				.column("name", DataTypes.ASCII);

		session.execute(CreateTableCqlGenerator.toCql(table));

		TableMetadata person = session.getMetadata().getKeyspace(getKeyspace()).flatMap(it -> it.getTable("person")).get();
		assertThat(person.getPartitionKey()).hasSize(2);
		assertThat(person.getClusteringColumns()).hasSize(2);
	}

	@Test // DATACASS-518
	void shouldGenerateTableWithClusterKeyAndOptions() {

		CreateTableSpecification table = CreateTableSpecification.createTable("person") //
				.partitionKeyColumn("id", DataTypes.ASCII) //
				.clusteredKeyColumn("date_of_birth", DataTypes.DATE, Ordering.ASCENDING) //
				.column("name", DataTypes.ASCII).with(TableOption.COMPACT_STORAGE);

		session.execute(CreateTableCqlGenerator.toCql(table));

		TableMetadata person = session.getMetadata().getKeyspace(getKeyspace()).flatMap(it -> it.getTable("person")).get();
		assertThat(person.getPartitionKey()).hasSize(1);
		assertThat(person.getClusteringColumns()).hasSize(1);
	}
}
