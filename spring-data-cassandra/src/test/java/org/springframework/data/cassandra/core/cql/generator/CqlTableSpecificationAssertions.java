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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.core.cql.keyspace.ColumnSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CqlStringUtils;
import org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.TableDescriptor;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;

/**
 * @author Matthew T. Adams
 * @author David Webb
 * @author Alex Shvid
 * @author Antoine Toulme
 */
public class CqlTableSpecificationAssertions {

	private static final Logger log = LoggerFactory.getLogger(CqlTableSpecificationAssertions.class);

	public static double DELTA = 1e-6; // delta for comparisons of doubles

	public static void assertTable(TableDescriptor expected, String keyspace, Session session) {
		TableMetadata tmd = session.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase())
				.getTable(expected.getName().getUnquoted()); // TODO: talk to Datastax about unquoting

		assertThat(expected.getName().getUnquoted()).isEqualTo(tmd.getName()); // TODO: talk to Datastax
		assertPartitionKeyColumns(expected, tmd);
		assertPrimaryKeyColumns(expected, tmd);
		assertColumns(expected.getColumns(), tmd.getColumns());
		assertOptions(expected.getOptions(), tmd.getOptions());
	}

	public static void assertNoTable(DropTableSpecification expected, String keyspace, Session session) {
		TableMetadata tmd = session.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase())
				.getTable(expected.getName().toCql());

		assertThat(tmd).isNull();
	}

	public static void assertPartitionKeyColumns(TableDescriptor expected, TableMetadata actual) {
		assertColumns(expected.getPartitionKeyColumns(), actual.getPartitionKey());
	}

	public static void assertPrimaryKeyColumns(TableDescriptor expected, TableMetadata actual) {
		assertColumns(expected.getPrimaryKeyColumns(), actual.getPrimaryKey());
	}

	public static void assertOptions(Map<String, Object> expected, TableOptionsMetadata actual) {

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
				assertThat((Double) expected).isCloseTo((Double) actual, offset(DELTA));
				return;

			case CACHING:
				assertCaching((Map<String, Object>) expected, (Map<String, String>) actual);
				return;

			case COMPACTION:
				assertCompaction((Map<String, Object>) expected, (Map<String, String>) actual);
				return;

			case COMPRESSION:
				assertCompression((Map<String, Object>) expected, (Map<String, String>) actual);
				return;
		}

		log.info(actual.getClass().getName());

		assertThat(
				tableOption.quotesValue() && !(actual instanceof CharSequence) ? CqlStringUtils.singleQuote(actual) : actual)
						.isEqualTo(expected);
	}

	public static void assertCaching(Map<String, Object> expected, Map<String, String> actual) {
		// TODO
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
	public static <T> T getOptionFor(TableOption option, Class<?> type, TableOptionsMetadata options) {
		switch (option) {
			case BLOOM_FILTER_FP_CHANCE:
				return (T) (Double) options.getBloomFilterFalsePositiveChance();
			case CACHING:
				return (T) options.getCaching();
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
		assertThat(expected.getName().toCql()).isEqualTo(actual.getName()); // TODO: expected.getName().getUnquoted()?
		assertThat(expected.getType()).isEqualTo(actual.getType());
	}
}
