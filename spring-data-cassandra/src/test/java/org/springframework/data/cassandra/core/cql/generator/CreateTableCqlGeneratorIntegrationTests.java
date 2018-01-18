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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTest;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

/**
 * Integration tests for {@link CreateTableCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class CreateTableCqlGeneratorIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	@Before
	public void setUp() {

		session.execute("DROP TABLE IF EXISTS person;");
		session.execute("DROP TABLE IF EXISTS address;");
	}

	@Test // DATACASS-518
	public void shouldGenerateSimpleTable() {

		CreateTableSpecification table = CreateTableSpecification.createTable("person") //
				.partitionKeyColumn("id", DataType.ascii()) //
				.clusteredKeyColumn("date_of_birth", DataType.date()) //
				.column("name", DataType.ascii());

		session.execute(CreateTableCqlGenerator.toCql(table));
	}

	@Test // DATACASS-518
	public void shouldGenerateTableWithClusterKeyOrdering() {

		CreateTableSpecification table = CreateTableSpecification.createTable("person") //
				.partitionKeyColumn("id", DataType.ascii()) //
				.partitionKeyColumn("country", DataType.ascii()) //
				.clusteredKeyColumn("date_of_birth", DataType.date(), Ordering.ASCENDING) //
				.clusteredKeyColumn("age", DataType.smallint()) //
				.column("name", DataType.ascii());

		session.execute(CreateTableCqlGenerator.toCql(table));

		TableMetadata person = cluster.getMetadata().getKeyspace(getKeyspace()).getTable("person");
		assertThat(person.getPartitionKey()).hasSize(2);
		assertThat(person.getClusteringColumns()).hasSize(2);
	}

	@Test // DATACASS-518
	public void shouldGenerateTableWithClusterKeyAndOptions() {

		CreateTableSpecification table = CreateTableSpecification.createTable("person") //
				.partitionKeyColumn("id", DataType.ascii()) //
				.clusteredKeyColumn("date_of_birth", DataType.date(), Ordering.ASCENDING) //
				.column("name", DataType.ascii()).with(TableOption.COMPACT_STORAGE);

		session.execute(CreateTableCqlGenerator.toCql(table));

		TableMetadata person = cluster.getMetadata().getKeyspace(getKeyspace()).getTable("person");
		assertThat(person.getPartitionKey()).hasSize(1);
		assertThat(person.getClusteringColumns()).hasSize(1);
	}
}
