package org.springframework.data.cassandra.cql;

import static org.springframework.data.cassandra.cql.CqlBuilder.createTable;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.cassandra.cql.CreateTable.Column.Order;

public class CreateTableTest {

	@Test
	public void createTableTest() {
		String name = "mytable";
		String type0 = "text";
		String partition0 = "partitionKey0";
		String partition1 = "partitionKey1";
		String primary0 = "primary0";
		String type1 = "text";
		String column1 = "column1";
		String type2 = "text";
		String column2 = "column2";
		Object value1 = null;
		String option1 = "COMPACT STORAGE";
		Object value2 = "this is a comment";
		String option2 = "comment";
		Object value3 = "0.00075";
		String option3 = "bloom_filter_fp_chance";
		Map<String, Object> value4 = new HashMap<String, Object>();
		value4.put("class", "LeveledCompactionStrategy");
		String option4 = "compaction";

		CreateTable builder = createTable().ifNotExists().name(name).partition(partition0, type0, Order.ASCENDING)
				.partition(partition1, type0, Order.DESCENDING).primary(primary0, type0).column(column1, type1)
				.column(column2, type2).option(option1, value1).option(option2, value2).option(option3, value3)
				.option(option4, value4);

		String cql = builder.cql();
		System.out.println(cql);
	}
}
