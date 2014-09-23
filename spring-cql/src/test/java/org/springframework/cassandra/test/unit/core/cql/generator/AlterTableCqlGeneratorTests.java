/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.unit.core.cql.generator;

import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;
import org.springframework.cassandra.core.keyspace.Option;
import org.springframework.cassandra.core.keyspace.TableOption;
import org.springframework.cassandra.core.keyspace.TableOption.CachingOption;
import org.springframework.cassandra.core.keyspace.TableOption.CompactionOption;
import org.springframework.cassandra.core.keyspace.TableOption.CompressionOption;

import com.datastax.driver.core.DataType;

public class AlterTableCqlGeneratorTests {

	private final static Logger log = LoggerFactory.getLogger(AlterTableCqlGeneratorTests.class);

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String tableName, String cql) {
		assertTrue(cql.startsWith("ALTER TABLE " + tableName + " "));
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 * 
	 * @param columnSpec IE, "foo text, bar blob"
	 */
	public static void assertColumnChanges(String columnSpec, String cql) {
		assertTrue(cql.contains(""));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	public static abstract class AlterTableTest extends
			TableOperationCqlGeneratorTest<AlterTableSpecification, AlterTableCqlGenerator> {}

	public static class BasicTest extends AlterTableTest {

		public String name = "mytable";
		public DataType alteredType = DataType.text();
		public String altered = "altered";

		public DataType addedType = DataType.text();
		public String added = "added";

		public String dropped = "dropped";

		@Override
		public AlterTableSpecification specification() {
			return AlterTableSpecification.alterTable().name(name).alter(altered, alteredType).add(added, addedType);
		}

		@Override
		public AlterTableCqlGenerator generator() {
			return new AlterTableCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumnChanges(
					String.format("ALTER %s TYPE %s, ADD %s %s, DROP %s", altered, alteredType, added, addedType, dropped), cql);
		}
	}

	/**
	 * Fully test all available create table options
	 * 
	 * @author David Webb
	 */
	public static class MultipleOptionsTest extends AlterTableTest {

		public String name = "timeseries_table";
		public DataType partitionKeyType0 = DataType.timeuuid();
		public String partitionKey0 = "tid";
		public DataType partitionKeyType1 = DataType.timestamp();
		public String partitionKey1 = "create_timestamp";
		public DataType columnType1 = DataType.text();
		public String column1 = "data_point";
		public Double readRepairChance = 0.6;
		public Double dcLocalReadRepairChance = 0.8;
		public Double bloomFilterFpChance = 0.002;
		public Boolean replcateOnWrite = Boolean.FALSE;
		public Long gcGraceSeconds = 1200l;
		public String comment = "This is My Table";
		public Map<Option, Object> compactionMap = new LinkedHashMap<Option, Object>();
		public Map<Option, Object> compressionMap = new LinkedHashMap<Option, Object>();

		@Override
		public AlterTableSpecification specification() {

			// Compaction
			compactionMap.put(CompactionOption.CLASS, "SizeTieredCompactionStrategy");
			compactionMap.put(CompactionOption.MIN_THRESHOLD, "4");
			// Compression
			compressionMap.put(CompressionOption.SSTABLE_COMPRESSION, "SnappyCompressor");
			compressionMap.put(CompressionOption.CHUNK_LENGTH_KB, 128);
			compressionMap.put(CompressionOption.CRC_CHECK_CHANCE, 0.75);

			return AlterTableSpecification
					.alterTable()
					.name(name)
					// .with(TableOption.COMPACT_STORAGE)
					.with(TableOption.READ_REPAIR_CHANCE, readRepairChance).with(TableOption.COMPACTION, compactionMap)
					.with(TableOption.COMPRESSION, compressionMap).with(TableOption.BLOOM_FILTER_FP_CHANCE, bloomFilterFpChance)
					.with(TableOption.CACHING, CachingOption.KEYS_ONLY).with(TableOption.REPLICATE_ON_WRITE, replcateOnWrite)
					.with(TableOption.COMMENT, comment).with(TableOption.DCLOCAL_READ_REPAIR_CHANCE, dcLocalReadRepairChance)
					.with(TableOption.GC_GRACE_SECONDS, gcGraceSeconds);
		}

		@Test
		public void test() {

			prepare();

			log.info(cql);

			assertPreamble(name, cql);
			// assertColumns(String.format("%s %s, %s %s, %s %s", partitionKey0, partitionKeyType0, partitionKey1,
			// partitionKeyType1, column1, columnType1), cql);
			// assertPrimaryKey(String.format("(%s, %s)", partitionKey0, partitionKey1), cql);
			// assertNullOption(TableOption.COMPACT_STORAGE.getName(), cql);
			// assertDoubleOption(TableOption.READ_REPAIR_CHANCE.getName(), readRepairChance, cql);
			// assertDoubleOption(TableOption.DCLOCAL_READ_REPAIR_CHANCE.getName(), dcLocalReadRepairChance, cql);
			// assertDoubleOption(TableOption.BLOOM_FILTER_FP_CHANCE.getName(), bloomFilterFpChance, cql);
			// assertStringOption(TableOption.CACHING.getName(), CachingOption.KEYS_ONLY.getValue(), cql);
			// assertStringOption(TableOption.REPLICATE_ON_WRITE.getName(), replcateOnWrite.toString(), cql);
			// assertStringOption(TableOption.COMMENT.getName(), comment, cql);
			// assertLongOption(TableOption.GC_GRACE_SECONDS.getName(), gcGraceSeconds, cql);

		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.test.unit.core.cql.generator.TableOperationCqlGeneratorTest#generator()
		 */
		@Override
		public AlterTableCqlGenerator generator() {
			return new AlterTableCqlGenerator(specification);
		}
	}
}
