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
package org.springframework.cassandra.test.integration.core.cql.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.cql.CqlStringUtils;
import org.springframework.cassandra.core.keyspace.ColumnSpecification;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.cassandra.core.keyspace.TableDescriptor;
import org.springframework.cassandra.core.keyspace.TableOption;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableMetadata.Options;

public class CqlTableSpecificationAssertions {

	private final static Logger log = LoggerFactory.getLogger(CqlTableSpecificationAssertions.class);

	public static double DELTA = 1e-6; // delta for comparisons of doubles

	public static void assertTable(TableDescriptor expected, String keyspace, Session session) {
		TableMetadata tmd = session.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase())
				.getTable(expected.getName().getUnquoted()); // TODO: talk to Datastax about unquoting

		assertEquals(expected.getName().getUnquoted(), tmd.getName()); // TODO: talk to Datastax
		assertPartitionKeyColumns(expected, tmd);
		assertPrimaryKeyColumns(expected, tmd);
		assertColumns(expected.getColumns(), tmd.getColumns());
		assertOptions(expected.getOptions(), tmd.getOptions());
	}

	public static void assertNoTable(DropTableSpecification expected, String keyspace, Session session) {
		TableMetadata tmd = session.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase())
				.getTable(expected.getName().toCql());

		assertNull(tmd);
	}

	public static void assertPartitionKeyColumns(TableDescriptor expected, TableMetadata actual) {
		assertColumns(expected.getPartitionKeyColumns(), actual.getPartitionKey());
	}

	public static void assertPrimaryKeyColumns(TableDescriptor expected, TableMetadata actual) {
		assertColumns(expected.getPrimaryKeyColumns(), actual.getPrimaryKey());
	}

	public static void assertOptions(Map<String, Object> expected, Options actual) {

		for (String key : expected.keySet()) {

			log.info(key + " -> " + expected.get(key));

			Object value = expected.get(key);
			TableOption tableOption = getTableOptionFor(key.toUpperCase());

			if (tableOption == null && key.equalsIgnoreCase(TableOption.COMPACT_STORAGE.getName())) {
				// TODO: figure out how to tell if COMPACT STORAGE was used
				continue;
			}

			assertOption(tableOption, key, value, getOptionFor(tableOption, tableOption.getType(), actual));
		}
	}

	@SuppressWarnings({ "unchecked", "incomplete-switch" })
	public static void assertOption(TableOption tableOption, String key, Object expected, Object actual) {

		if (tableOption == null) { // then this is a string-only or unknown value
			key.equalsIgnoreCase(actual.toString()); // TODO: determine if this is the right test
		}

		switch (tableOption) {

			case BLOOM_FILTER_FP_CHANCE:
			case READ_REPAIR_CHANCE:
			case DCLOCAL_READ_REPAIR_CHANCE:
				assertEquals((Double) expected, (Double) actual, DELTA);
				return;

			case CACHING:
				assertEquals(((String) expected).toUpperCase(), ((String) actual).toUpperCase());
				return;

			case COMPACTION:
				assertCompaction((Map<String, Object>) expected, (Map<String, String>) actual);
				return;

			case COMPRESSION:
				assertCompression((Map<String, Object>) expected, (Map<String, String>) actual);
				return;
		}

		log.info(actual.getClass().getName());

		assertEquals(expected,
				tableOption.quotesValue() && !(actual instanceof CharSequence) ? CqlStringUtils.singleQuote(actual) : actual);
	}

	public static void assertCompaction(Map<String, Object> expected, Map<String, String> actual) {
		// TODO
	}

	public static void assertCompression(Map<String, Object> expected, Map<String, String> actual) {
		// TODO
	}

	public static TableOption getTableOptionFor(String key) {
		try {
			return TableOption.valueOf(key);
		} catch (IllegalArgumentException x) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T getOptionFor(TableOption option, Class<?> type, Options options) {
		switch (option) {
			case BLOOM_FILTER_FP_CHANCE:
				return (T) (Double) options.getBloomFilterFalsePositiveChance();
			case CACHING:
				return (T) CqlStringUtils.singleQuote(options.getCaching());
			case COMMENT:
				return (T) CqlStringUtils.singleQuote(options.getComment());
			case COMPACTION:
				return (T) options.getCompaction();
			case COMPACT_STORAGE:
				throw new Error(); // TODO: figure out
			case COMPRESSION:
				return (T) options.getCompression();
			case DCLOCAL_READ_REPAIR_CHANCE:
				return (T) (Double) options.getLocalReadRepairChance();
			case GC_GRACE_SECONDS:
				return (T) new Long(options.getGcGraceInSeconds());
			case READ_REPAIR_CHANCE:
				return (T) (Double) options.getReadRepairChance();
			case REPLICATE_ON_WRITE:
				return (T) (Boolean) options.getReplicateOnWrite();
		}
		return null;
	}

	public static void assertColumns(List<ColumnSpecification> expected, List<ColumnMetadata> actual) {
		for (int i = 0; i < expected.size(); i++) {
			ColumnSpecification expectedColumn = expected.get(i);
			ColumnMetadata actualColumn = actual.get(i);

			assertColumn(expectedColumn, actualColumn);
		}
	}

	public static void assertColumn(ColumnSpecification expected, ColumnMetadata actual) {
		assertEquals(expected.getName().toCql(), actual.getName()); // TODO: expected.getName().getUnquoted()?
		assertEquals(expected.getType(), actual.getType());
	}
}
