/*
 * Copyright 2016-2025 the original author or authors.
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

import java.time.LocalDate;
import java.util.Collection;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.generator.CqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.SpecificationBuilder;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.core.mapping.Table;
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
 * @author Mikhail Polivakha
 * @author Seungho Kang
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
					.execute(CqlGenerator.toCql(SpecificationBuilder.dropTable(table.getName())));
		}
	}

	private KeyspaceMetadata getKeyspaceMetadata() {
		Metadata metadata = getSession().getMetadata();
		return getSession().getKeyspace().flatMap(metadata::getKeyspace).get();
	}

	@Test // GH-359, GH-1584
	void shouldApplyTableOptions() {

		Map<String, Object> options = Map.of(TableOption.COMMENT.getName(), "This is comment for table", //
				TableOption.BLOOM_FILTER_FP_CHANCE.getName(), "0.3", //
				TableOption.DEFAULT_TIME_TO_LIVE.getName(), "864000", //
				TableOption.CDC.getName(), true, //
				TableOption.SPECULATIVE_RETRY.getName(), "90percentile", //
				TableOption.MEMTABLE_FLUSH_PERIOD_IN_MS.getName(), "1000", //
				TableOption.CRC_CHECK_CHANCE.getName(), "0.9", //
				TableOption.MIN_INDEX_INTERVAL.getName(), "128", //
				TableOption.MAX_INDEX_INTERVAL.getName(), "2048", //
				TableOption.READ_REPAIR.getName(), "BLOCKING");

		CqlIdentifier tableName = CqlIdentifier.fromCql("someTable");
		cassandraAdminTemplate.createTable(true, tableName, SomeTable.class, options);

		TableMetadata someTable = getKeyspaceMetadata().getTables().get(tableName);

		assertThat(someTable).isNotNull();
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.COMMENT.getName()))) //
				.isEqualTo("This is comment for table");
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.BLOOM_FILTER_FP_CHANCE.getName()))) //
				.isEqualTo(0.3);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.DEFAULT_TIME_TO_LIVE.getName()))) //
				.isEqualTo(864_000);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.SPECULATIVE_RETRY.getName()))) //
				.isEqualTo("90p");
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.MEMTABLE_FLUSH_PERIOD_IN_MS.getName()))) //
				.isEqualTo(1000);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.CRC_CHECK_CHANCE.getName()))) //
				.isEqualTo(0.9);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.MIN_INDEX_INTERVAL.getName()))) //
				.isEqualTo(128);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.MAX_INDEX_INTERVAL.getName()))) //
				.isEqualTo(2048);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.READ_REPAIR.getName()))) //
				.isEqualTo("BLOCKING");
	}

	@Test // GH-359, GH-1584
	void shouldApplyTableOptions_with_raw() {

		Map<String, Object> options = Map.of(TableOption.COMMENT.getName(), "This is comment for table", //
				"bloom_filter_fp_chance", "0.3", //
				"default_time_to_live", "864000", //
				"cdc", true, //
				"speculative_retry", "90percentile", //
				"memtable_flush_period_in_ms", "1000", //
				"crc_check_chance", "0.9", //
				"min_index_interval", "128", //
				"max_index_interval", "2048", //
				"read_repair", "BLOCKING");

		CqlIdentifier tableName = CqlIdentifier.fromCql("someTable");
		cassandraAdminTemplate.createTable(true, tableName, SomeTable.class, options);

		TableMetadata someTable = getKeyspaceMetadata().getTables().get(tableName);

		assertThat(someTable).isNotNull();
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.COMMENT.getName()))) //
				.isEqualTo("This is comment for table");
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.BLOOM_FILTER_FP_CHANCE.getName()))) //
				.isEqualTo(0.3);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.DEFAULT_TIME_TO_LIVE.getName()))) //
				.isEqualTo(864_000);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.SPECULATIVE_RETRY.getName()))) //
				.isEqualTo("90p");
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.MEMTABLE_FLUSH_PERIOD_IN_MS.getName()))) //
				.isEqualTo(1000);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.CRC_CHECK_CHANCE.getName()))) //
				.isEqualTo(0.9);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.MIN_INDEX_INTERVAL.getName()))) //
				.isEqualTo(128);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.MAX_INDEX_INTERVAL.getName()))) //
				.isEqualTo(2048);
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.READ_REPAIR.getName()))) //
				.isEqualTo("BLOCKING");
	}

	@Test // GH-1388
	void shouldCreateTableWithNameDerivedFromEntityClass() {

		cassandraAdminTemplate.createTable(true, SomeTable.class, Map.of(TableOption.COMMENT.getName(),
				"This is comment for table", TableOption.BLOOM_FILTER_FP_CHANCE.getName(), "0.3"));

		TableMetadata someTable = getKeyspaceMetadata().getTables().values().stream().findFirst().orElse(null);

		assertThat(someTable).isNotNull();
		assertThat(someTable.getOptions().get(CqlIdentifier.fromCql(TableOption.COMMENT.getName())))
				.isEqualTo("This is comment for table");
	}

	@Test // DATACASS-173
	void testCreateTables() {

		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);

		cassandraAdminTemplate.createTable(true, User.class);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);

		cassandraAdminTemplate.createTable(true, User.class);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);
	}

	@Test
	void testDropTable() {

		cassandraAdminTemplate.createTable(true, User.class);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(1);

		cassandraAdminTemplate.dropTable(User.class);
		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);

		cassandraAdminTemplate.createTable(true, User.class);
		cassandraAdminTemplate.dropTable(CqlIdentifier.fromCql("users"));

		assertThat(getKeyspaceMetadata().getTables()).hasSize(0);
	}

	@Table("someTable")
	private static class SomeTable {

		@Id private String name;
		private Integer number;
		private LocalDate createdAt;
	}
}
