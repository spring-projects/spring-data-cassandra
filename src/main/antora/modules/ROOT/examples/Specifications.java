/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.cassandra.example;

public class Specifications {

	public void document() {
		// tag::keyspace[]

		CqlSpecification createKeyspace = SpecificationBuilder.createKeyspace("my_keyspace")
				.with(KeyspaceOption.REPLICATION, KeyspaceAttributes.newSimpleReplication())
				.with(KeyspaceOption.DURABLE_WRITES, true);

		// results in CREATE KEYSPACE my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true
		String cql = CqlGenerator.toCql(createKeyspace);
		// end::keyspace[]

		// tag::table[]

		CreateTableSpecification createTable = CreateTableSpecification.createTable("my_table")
				.partitionKeyColumn("last_name", DataTypes.TEXT)
				.partitionKeyColumn("first_name", DataTypes.TEXT)
				.column("age", DataTypes.INT);

		// results in CREATE TABLE my_table (last_name text, first_name text, age int, PRIMARY KEY(last_name, first_name))
		String cql = CqlGenerator.toCql(createTable);
		// end::table[]

		// tag::index[]

		CreateIndexSpecification spec = SpecificationBuilder.createIndex()
				.tableName("mytable").keys().columnName("column");

		// results in CREATE INDEX ON mytable (KEYS(column))
		String cql = CqlGenerator.toCql(createTable);
		// end::index[]
	}

}
