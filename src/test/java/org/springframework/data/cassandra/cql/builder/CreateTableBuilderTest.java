package org.springframework.data.cassandra.cql.builder;

import static junit.framework.Assert.assertEquals;
import static org.springframework.data.cassandra.cql.builder.CqlBuilder.createTable;
import static org.springframework.data.cassandra.cql.builder.MapBuilder.map;
import static org.springframework.data.cassandra.cql.builder.TableOption.BLOOM_FILTER_FP_CHANCE;
import static org.springframework.data.cassandra.cql.builder.TableOption.CACHING;
import static org.springframework.data.cassandra.cql.builder.TableOption.COMMENT;
import static org.springframework.data.cassandra.cql.builder.TableOption.COMPACTION;
import static org.springframework.data.cassandra.cql.builder.TableOption.CompactionOption.TOMBSTONE_THRESHOLD;

import org.junit.Test;
import org.springframework.data.cassandra.cql.builder.TableOption.CachingOption;

import com.datastax.driver.core.DataType;

public class CreateTableBuilderTest {

	@Test
	public void createTableTest() {
		String name = "my\"\"table";
		DataType type0 = DataType.text();
		String partKey0 = "partitionKey0";
		String partition1 = "partitionKey1";
		String primary0 = "primary0";
		DataType type1 = DataType.text();
		String column1 = "column1";
		DataType type2 = DataType.bigint();
		String column2 = "column2";
		Object comment = "this is a comment";
		Object bloom = "0.00075";
		Object caching = CachingOption.KEYS_ONLY;

		CreateTableBuilder builder = createTable().ifNotExists().name(name).partitionKeyColumn(partKey0, type0)
				.partitionKeyColumn(partition1, type0).primaryKeyColumn(primary0, type0).column(column1, type1)
				.column(column2, type2).with(COMMENT, comment).with(BLOOM_FILTER_FP_CHANCE, bloom)
				.with(COMPACTION, map().entry(TOMBSTONE_THRESHOLD, "0.15")).with(CACHING, caching);

		String cql = builder.toCql();
		assertEquals(
				"CREATE TABLE IF NOT EXISTS \"my\"\"table\" (partitionKey0 text, partitionKey1 text, primary0 text, column1 text, column2 bigint, PRIMARY KEY ((partitionKey0, partitionKey1), primary0) WITH CLUSTERING ORDER BY (primary0 ASC) AND comment = 'this is a comment' AND bloom_filter_fp_chance = 0.00075 AND compaction = { 'tombstone_threshold' : 0.15 } AND caching = keys_only;",
				cql);
	}
}
