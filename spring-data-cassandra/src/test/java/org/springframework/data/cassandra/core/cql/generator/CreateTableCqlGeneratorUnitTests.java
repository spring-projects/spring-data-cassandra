/*
 * Copyright 2016-2017 the original author or authors.
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
import static org.springframework.data.cassandra.core.cql.CqlIdentifier.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.ReservedKeyword;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CachingOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CompactionOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CompressionOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.KeyCachingOption;

import com.datastax.driver.core.DataType;

/**
 * Unit tests for {@link CreateTableCqlGenerator}.
 *
 * @author Matthew T. Adams
 * @author David Webb
 */
public class CreateTableCqlGeneratorUnitTests {

	private static final Logger log = LoggerFactory.getLogger(CreateTableCqlGeneratorUnitTests.class);

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(CqlIdentifier tableName, String cql) {
		assertThat(cql.startsWith("CREATE TABLE " + tableName + " ")).isTrue();
	}

	/**
	 * Asserts that the given primary key definition is contained in the given CQL string properly.
	 *
	 * @param primaryKeyString IE, "foo", "foo, bar, baz", "(foo, bar), baz", etc
	 */
	public static void assertPrimaryKey(String primaryKeyString, String cql) {
		assertThat(cql.contains(", PRIMARY KEY (" + primaryKeyString + "))")).isTrue();
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 *
	 * @param columnSpec IE, "foo text, bar blob"
	 */
	public static void assertColumns(String columnSpec, String cql) {
		assertThat(cql.contains("(" + columnSpec + ",")).isTrue();
	}

	/**
	 * Asserts that the read repair change is set properly
	 */
	public static void assertStringOption(String name, String value, String cql) {
		log.info(name + " -> " + value);
		assertThat(cql.contains(name + " = '" + value + "'")).isTrue();
	}

	/**
	 * Asserts that the option is set
	 */
	public static void assertDoubleOption(String name, Double value, String cql) {
		log.info(name + " -> " + value);
		assertThat(cql.contains(name + " = " + value)).isTrue();
	}

	public static void assertLongOption(String name, Long value, String cql) {
		log.info(name + " -> " + value);
		assertThat(cql.contains(name + " = " + value)).isTrue();
	}

	/**
	 * Asserts that the read repair change is set properly
	 */
	public static void assertNullOption(String name, String cql) {
		log.info(name);
		assertThat(cql.contains(" " + name + " ")).isTrue();
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations or
	 * {@link #generator()} method.
	 */
	public static abstract class CreateTableTest
			extends AbstractTableOperationCqlGeneratorTest<CreateTableSpecification, CreateTableCqlGenerator> {

		@Override
		public CreateTableCqlGenerator generator() {
			return new CreateTableCqlGenerator(specification);
		}
	}

	public static class BasicTest extends CreateTableTest {

		public CqlIdentifier name = of("mytable");
		public DataType partitionKeyType0 = DataType.text();
		public CqlIdentifier partitionKey0 = of("partitionKey0");
		public DataType columnType1 = DataType.text();
		public String column1 = "column1";

		@Override
		public CreateTableSpecification specification() {
			return CreateTableSpecification.createTable(name).partitionKeyColumn(partitionKey0, partitionKeyType0)
					.column(column1, columnType1);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumns(String.format("%s %s, %s %s", partitionKey0, partitionKeyType0, column1, columnType1), cql);
			assertPrimaryKey(partitionKey0.toCql(), cql);
		}
	}

	public static class CompositePartitionKeyTest extends CreateTableTest {

		public CqlIdentifier name = of("composite_partition_key_table");
		public DataType partKeyType0 = DataType.text();
		public CqlIdentifier partKey0 = of("partKey0");
		public DataType partKeyType1 = DataType.text();
		public CqlIdentifier partKey1 = of("partKey1");
		public CqlIdentifier column0 = of("column0");
		public DataType columnType0 = DataType.text();

		@Override
		public CreateTableSpecification specification() {
			return CreateTableSpecification.createTable(name).partitionKeyColumn(partKey0, partKeyType0)
					.partitionKeyColumn(partKey1, partKeyType1).column(column0, columnType0);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumns(
					String.format("%s %s, %s %s, %s %s", partKey0, partKeyType0, partKey1, partKeyType1, column0, columnType0),
					cql);
			assertPrimaryKey(String.format("(%s, %s)", partKey0, partKey1), cql);
		}
	}

	/**
	 * Test just the Read Repair Chance
	 *
	 * @author David Webb
	 */
	public static class ReadRepairChanceTest extends CreateTableTest {

		public CqlIdentifier name = of("mytable");
		public DataType partitionKeyType0 = DataType.text();
		public CqlIdentifier partitionKey0 = of("partitionKey0");
		public DataType partitionKeyType1 = DataType.timestamp();
		public CqlIdentifier partitionKey1 = of("create_timestamp");
		public DataType columnType1 = DataType.text();
		public CqlIdentifier column1 = of("column1");
		public Double readRepairChance = 0.5;

		@Override
		public CreateTableSpecification specification() {
			return CreateTableSpecification.createTable(name).partitionKeyColumn(partitionKey0, partitionKeyType0)
					.partitionKeyColumn(partitionKey1, partitionKeyType1).column(column1, columnType1)
					.with(TableOption.READ_REPAIR_CHANCE, readRepairChance);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumns(String.format("%s %s, %s %s, %s %s", partitionKey0, partitionKeyType0, partitionKey1,
					partitionKeyType1, column1, columnType1), cql);
			assertPrimaryKey(String.format("(%s, %s)", partitionKey0, partitionKey1), cql);
			assertDoubleOption(TableOption.READ_REPAIR_CHANCE.getName(), readRepairChance, cql);
		}
	}

	/**
	 * Fully test all available create table options
	 *
	 * @author David Webb
	 */
	public static class MultipleOptionsTest extends CreateTableTest {

		public CqlIdentifier name = of("timeseries_table");
		public DataType partitionKeyType0 = DataType.timeuuid();
		public CqlIdentifier partitionKey0 = of("tid");
		public DataType partitionKeyType1 = DataType.timestamp();
		public CqlIdentifier partitionKey1 = of("create_timestamp");
		public DataType columnType1 = DataType.text();
		public CqlIdentifier column1 = of("data_point");
		public Double readRepairChance = 0.5;
		public Double dcLocalReadRepairChance = 0.7;
		public Double bloomFilterFpChance = 0.001;
		public Boolean replcateOnWrite = Boolean.FALSE;
		public Long gcGraceSeconds = 600l;
		public String comment = "This is My Table";
		public Map<Option, Object> compactionMap = new LinkedHashMap<>();
		public Map<Option, Object> compressionMap = new LinkedHashMap<>();
		public Map<Option, Object> cachingMap = new LinkedHashMap<>();

		@Override
		public CreateTableSpecification specification() {

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

			return CreateTableSpecification.createTable(name).partitionKeyColumn(partitionKey0, partitionKeyType0)
					.partitionKeyColumn(partitionKey1, partitionKeyType1).column(column1, columnType1)
					.with(TableOption.COMPACT_STORAGE).with(TableOption.READ_REPAIR_CHANCE, readRepairChance)
					.with(TableOption.COMPACTION, compactionMap).with(TableOption.COMPRESSION, compressionMap)
					.with(TableOption.BLOOM_FILTER_FP_CHANCE, bloomFilterFpChance).with(TableOption.CACHING, cachingMap)
					.with(TableOption.COMMENT, comment).with(TableOption.DCLOCAL_READ_REPAIR_CHANCE, dcLocalReadRepairChance)
					.with(TableOption.GC_GRACE_SECONDS, gcGraceSeconds);
		}

		@Test
		public void test() {

			prepare();

			log.info(cql);

			assertPreamble(name, cql);
			assertColumns(String.format("%s %s, %s %s, %s %s", partitionKey0, partitionKeyType0, partitionKey1,
					partitionKeyType1, column1, columnType1), cql);
			assertPrimaryKey(String.format("(%s, %s)", partitionKey0, partitionKey1), cql);
			assertNullOption(TableOption.COMPACT_STORAGE.getName(), cql);
			assertDoubleOption(TableOption.READ_REPAIR_CHANCE.getName(), readRepairChance, cql);
			assertDoubleOption(TableOption.DCLOCAL_READ_REPAIR_CHANCE.getName(), dcLocalReadRepairChance, cql);
			assertDoubleOption(TableOption.BLOOM_FILTER_FP_CHANCE.getName(), bloomFilterFpChance, cql);
			assertStringOption(TableOption.COMMENT.getName(), comment, cql);
			assertLongOption(TableOption.GC_GRACE_SECONDS.getName(), gcGraceSeconds, cql);

		}
	}

	public static class FunkyTableNameTest {

		public static final List<String> FUNKY_LEGAL_NAMES;

		static {
			List<String> funkies = new ArrayList<>(Arrays.asList(new String[] { /* TODO */ }));
			// TODO: should these work? "a \"\" x", "a\"\"\"\"x", "a b"
			FUNKY_LEGAL_NAMES = Collections.unmodifiableList(Arrays.stream(ReservedKeyword.values()) //
					.map(Enum::name) //
					.collect(Collectors.toList()));
		}

		@Test
		public void test() {
			for (String name : FUNKY_LEGAL_NAMES) {
				new TableNameTest(name).test();
			}
		}
	}

	/**
	 * This class is supposed to be used by other test classes.
	 */
	public static class TableNameTest extends CreateTableTest {

		public String tableName;

		public TableNameTest(String tableName) {
			this.tableName = tableName;
		}

		@Override
		public CreateTableSpecification specification() {
			return CreateTableSpecification.createTable(tableName).partitionKeyColumn(of("pk"), DataType.text());
		}

		/**
		 * There is no @Test annotation on this method on purpose! It's supposed to be called by another test class's @Test
		 * method so that you can loop, calling this test method as many times as are necessary.
		 */
		public void test() {
			prepare();
			assertPreamble(of(tableName), cql);
		}
	}
}
