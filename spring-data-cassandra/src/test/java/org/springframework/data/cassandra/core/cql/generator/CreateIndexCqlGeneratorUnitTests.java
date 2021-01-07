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
package org.springframework.data.cassandra.core.cql.generator;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;

/**
 * Unit tests for {@link CreateIndexCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
class CreateIndexCqlGeneratorUnitTests {

	@Test // DATACASS-213
	void createIndex() {

		CreateIndexSpecification spec = CreateIndexSpecification.createIndex("myindex").tableName("mytable")
				.columnName("column");

		assertThat(CreateIndexCqlGenerator.toCql(spec)).isEqualTo("CREATE INDEX myindex ON mytable (column);");
	}

	@Test // DATACASS-213
	void createCustomIndex() {

		CreateIndexSpecification spec = CreateIndexSpecification.createIndex("myindex").tableName("mytable")
				.columnName("column").using("indexclass");

		assertThat(CreateIndexCqlGenerator.toCql(spec))
				.isEqualTo("CREATE CUSTOM INDEX myindex ON mytable (column) USING 'indexclass';");
	}

	@Test // DATACASS-213
	void createIndexOnKeys() {

		CreateIndexSpecification spec = CreateIndexSpecification.createIndex().tableName("mytable").keys()
				.columnName("column");

		assertThat(CreateIndexCqlGenerator.toCql(spec)).isEqualTo("CREATE INDEX ON mytable (KEYS(column));");
	}

	@Test // DATACASS-213
	void createIndexIfNotExists() {

		CreateIndexSpecification spec = CreateIndexSpecification.createIndex().tableName("mytable").columnName("column")
				.ifNotExists();

		assertThat(CreateIndexCqlGenerator.toCql(spec)).isEqualTo("CREATE INDEX IF NOT EXISTS ON mytable (column);");
	}

	@Test // DATACASS-306
	void createIndexWithOptions() {

		CreateIndexSpecification spec = CreateIndexSpecification.createIndex().tableName("mytable").columnName("column")
				.withOption("foo", "b'a'r").withOption("type", "PREFIX");

		assertThat(CreateIndexCqlGenerator.toCql(spec))
				.isEqualTo("CREATE INDEX ON mytable (column) WITH OPTIONS = {'foo': 'b''a''r', 'type': 'PREFIX'};");
	}
}
