package org.springframework.data.cassandra.cql;

import static org.springframework.data.cassandra.cql.builder.CqlBuilder.createTable;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.cassandra.cql.builder.CreateTableBuilder;

import com.datastax.driver.core.DataType;

public class CreateTableBuilderTest {

	@Test
	public void createTableTest() {
		String name = "mytable";
		DataType type0 = DataType.text();
		String partition0 = "partitionKey0";
		String partition1 = "partitionKey1";
		String primary0 = "primary0";
		DataType type1 = DataType.text();
		String column1 = "column1";
		DataType type2 = DataType.bigint();
		String column2 = "column2";
		Object value2 = "this is a comment";
		String option2 = "comment";
		Object value3 = "0.00075";
		String option3 = "bloom_filter_fp_chance";
		Map<String, Object> value4 = new HashMap<String, Object>();
		value4.put("class", "LeveledCompactionStrategy");
		String option4 = "compaction";

		CreateTableBuilder builder = createTable().ifNotExists().name(name).partitionKeyColumn(partition0, type0)
				.partitionKeyColumn(partition1, type0).primaryKeyColumn(primary0, type0).column(column1, type1)
				.column(column2, type2).withQuoted(option2, value2).withUnquoted(option3, value3).with(option4, value4)
				.withCompactStorage();

		String cql = builder.toCql();
		System.out.println(cql);
	}
}
