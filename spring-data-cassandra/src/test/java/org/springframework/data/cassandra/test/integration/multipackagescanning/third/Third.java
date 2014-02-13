package org.springframework.data.cassandra.test.integration.multipackagescanning.third;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class Third {

	@PrimaryKey
	String key;
}
