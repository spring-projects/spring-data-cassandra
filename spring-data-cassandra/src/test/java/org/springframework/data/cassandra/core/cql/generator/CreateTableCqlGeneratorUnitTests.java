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
import static org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGenerator.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CachingOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CompactionOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CompressionOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.KeyCachingOption;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

/**
 * Unit tests for {@link CreateTableCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
class CreateTableCqlGeneratorUnitTests {

	@Test
	void shouldGenerateCorrectCQL() {

		CqlIdentifier name = CqlIdentifier.fromCql("mytable");
		DataType partitionKeyType0 = DataTypes.TEXT;
		CqlIdentifier partitionKey0 = CqlIdentifier.fromCql("partitionKey0");
		DataType columnType1 = DataTypes.TEXT;
		String column1 = "column1";

		CreateTableSpecification table = CreateTableSpecification.createTable(name)
				.partitionKeyColumn(partitionKey0, partitionKeyType0).column(column1, columnType1);

		String cql = toCql(table);
		assertPreamble(name, cql);
		assertColumns("partitionkey0 text, column1 text", cql);
		assertPrimaryKey(partitionKey0.toString(), cql);
	}

	@Test
	void shouldGenerateCompositePrimaryKey() {

		CqlIdentifier name = CqlIdentifier.fromCql("composite_partition_key_table");
		DataType partKeyType0 = DataTypes.TEXT;
		CqlIdentifier partKey0 = CqlIdentifier.fromCql("partKey0");
		DataType partKeyType1 = DataTypes.TEXT;
		CqlIdentifier partKey1 = CqlIdentifier.fromCql("partKey1");
		CqlIdentifier column0 = CqlIdentifier.fromCql("column0");
		DataType columnType0 = DataTypes.TEXT;

		CreateTableSpecification table = CreateTableSpecification.createTable(name)
				.partitionKeyColumn(partKey0, partKeyType0).partitionKeyColumn(partKey1, partKeyType1)
				.column(column0, columnType0);

		String cql = toCql(table);

		assertPreamble(name, cql);
		assertColumns("partkey0 text, partkey1 text, column0 text", cql);
		assertPrimaryKey(String.format("(%s, %s)", partKey0, partKey1), cql);
	}

	@Test
	void shouldGenerateTableOptions() {

		CqlIdentifier name = CqlIdentifier.fromCql("mytable");
		DataType partitionKeyType0 = DataTypes.TEXT;
		CqlIdentifier partitionKey0 = CqlIdentifier.fromCql("partitionKey0");
		DataType partitionKeyType1 = DataTypes.TIMESTAMP;
		CqlIdentifier partitionKey1 = CqlIdentifier.fromCql("create_timestamp");
		DataType columnType1 = DataTypes.TEXT;
		CqlIdentifier column1 = CqlIdentifier.fromCql("column1");
		Double readRepairChance = 0.5;

		CreateTableSpecification table = CreateTableSpecification.createTable(name)
				.partitionKeyColumn(partitionKey0, partitionKeyType0).partitionKeyColumn(partitionKey1, partitionKeyType1)
				.column(column1, columnType1).with(TableOption.READ_REPAIR_CHANCE, readRepairChance);

		String cql = toCql(table);

		assertPreamble(name, cql);
		assertColumns("partitionkey0 text, create_timestamp timestamp, column1 text", cql);
		assertPrimaryKey(String.format("(%s, %s)", partitionKey0, partitionKey1), cql);
		assertDoubleOption(TableOption.READ_REPAIR_CHANCE.getName(), readRepairChance, cql);
	}

	@Test
	void shouldGenerateMultipleOptions() {

		CqlIdentifier name = CqlIdentifier.fromCql("timeseries_table");
		DataType partitionKeyType0 = DataTypes.TIMEUUID;
		CqlIdentifier partitionKey0 = CqlIdentifier.fromCql("tid");
		DataType partitionKeyType1 = DataTypes.TIMESTAMP;
		CqlIdentifier partitionKey1 = CqlIdentifier.fromCql("create_timestamp");
		DataType columnType1 = DataTypes.TEXT;
		CqlIdentifier column1 = CqlIdentifier.fromCql("data_point");
		Double readRepairChance = 0.5;
		Double dcLocalReadRepairChance = 0.7;
		Double bloomFilterFpChance = 0.001;
		Boolean replcateOnWrite = Boolean.FALSE;
		Long gcGraceSeconds = 600l;
		String comment = "This is My Table";
		Map<Option, Object> compactionMap = new LinkedHashMap<>();
		Map<Option, Object> compressionMap = new LinkedHashMap<>();
		Map<Option, Object> cachingMap = new LinkedHashMap<>();

		// Compaction
		compactionMap.put(CompactionOption.CLASS, "SizeTieredCompactionStrategy");
		compactionMap.put(CompactionOption.MIN_THRESHOLD, "4");
		// Compression
		compressionMap.put(CompressionOption.SSTABLE_COMPRESSION, "SnappyCompressor");
		compressionMap.put(CompressionOption.CHUNK_LENGTH_KB, 128);
		compressionMap.put(CompressionOption.CRC_CHECK_CHANCE, 0.75);
		// Caching
		cachingMap.put(CachingOption.KEYS, KeyCachingOption.ALL);
		cachingMap.put(CachingOption.ROWS_PER_PARTITION, "NONE");

		CreateTableSpecification table = CreateTableSpecification.createTable(name)
				.partitionKeyColumn(partitionKey0, partitionKeyType0).partitionKeyColumn(partitionKey1, partitionKeyType1)
				.column(column1, columnType1).with(TableOption.COMPACT_STORAGE)
				.with(TableOption.READ_REPAIR_CHANCE, readRepairChance).with(TableOption.COMPACTION, compactionMap)
				.with(TableOption.COMPRESSION, compressionMap).with(TableOption.BLOOM_FILTER_FP_CHANCE, bloomFilterFpChance)
				.with(TableOption.CACHING, cachingMap).with(TableOption.COMMENT, comment)
				.with(TableOption.DCLOCAL_READ_REPAIR_CHANCE, dcLocalReadRepairChance)
				.with(TableOption.GC_GRACE_SECONDS, gcGraceSeconds);

		String cql = toCql(table);

		assertPreamble(name, cql);
		assertColumns("tid timeuuid, create_timestamp timestamp, data_point text", cql);
		assertPrimaryKey(String.format("(%s, %s)", partitionKey0, partitionKey1), cql);
		assertNullOption(TableOption.COMPACT_STORAGE.getName(), cql);
		assertDoubleOption(TableOption.READ_REPAIR_CHANCE.getName(), readRepairChance, cql);
		assertDoubleOption(TableOption.DCLOCAL_READ_REPAIR_CHANCE.getName(), dcLocalReadRepairChance, cql);
		assertDoubleOption(TableOption.BLOOM_FILTER_FP_CHANCE.getName(), bloomFilterFpChance, cql);
		assertStringOption(TableOption.COMMENT.getName(), comment, cql);
		assertLongOption(TableOption.GC_GRACE_SECONDS.getName(), gcGraceSeconds, cql);
	}

	@Test // DATACASS-518
	void createTableWithOrderedClustering() {

		CreateTableSpecification table = CreateTableSpecification.createTable("person") //
				.partitionKeyColumn("id", DataTypes.ASCII) //
				.clusteredKeyColumn("date_of_birth", DataTypes.DATE, Ordering.ASCENDING) //
				.column("name", DataTypes.ASCII);

		assertThat(toCql(table)).isEqualTo("CREATE TABLE person (id ascii, date_of_birth date, name ascii, " //
				+ "PRIMARY KEY (id, date_of_birth)) " //
				+ "WITH CLUSTERING ORDER BY (date_of_birth ASC);");
	}

	@Test // DATACASS-518
	void createTableWithOrderedClusteringAndOptions() {

		CreateTableSpecification table = CreateTableSpecification.createTable("person") //
				.partitionKeyColumn("id", DataTypes.ASCII) //
				.clusteredKeyColumn("date_of_birth", DataTypes.DATE, Ordering.ASCENDING) //
				.column("name", DataTypes.ASCII).with(TableOption.COMPACT_STORAGE);

		assertThat(toCql(table)).isEqualTo("CREATE TABLE person (id ascii, date_of_birth date, name ascii, " //
				+ "PRIMARY KEY (id, date_of_birth)) " //
				+ "WITH CLUSTERING ORDER BY (date_of_birth ASC) AND COMPACT STORAGE;");
	}

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	private static void assertPreamble(CqlIdentifier tableName, String cql) {
		assertThat(cql).startsWith("CREATE TABLE " + tableName + " ");
	}

	/**
	 * Asserts that the given primary key definition is contained in the given CQL string properly.
	 *
	 * @param primaryKeyString IE, "foo", "foo, bar, baz", "(foo, bar), baz", etc
	 */
	private static void assertPrimaryKey(String primaryKeyString, String cql) {
		assertThat(cql).contains(", PRIMARY KEY (" + primaryKeyString + "))");
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 *
	 * @param columnSpec IE, "foo text, bar blob"
	 */
	private static void assertColumns(String columnSpec, String cql) {
		assertThat(cql).contains("(" + columnSpec + ",");
	}

	/**
	 * Asserts that the read repair change is set properly
	 */
	private static void assertStringOption(String name, String value, String cql) {
		assertThat(cql).contains(name + " = '" + value + "'");
	}

	/**
	 * Asserts that the option is set
	 */
	private static void assertDoubleOption(String name, Double value, String cql) {
		assertThat(cql).contains(name + " = " + value);
	}

	private static void assertLongOption(String name, Long value, String cql) {
		assertThat(cql).contains(name + " = " + value);
	}

	/**
	 * Asserts that the read repair change is set properly
	 */
	private static void assertNullOption(String name, String cql) {
		assertThat(cql).contains(" " + name + " ");
	}
}
