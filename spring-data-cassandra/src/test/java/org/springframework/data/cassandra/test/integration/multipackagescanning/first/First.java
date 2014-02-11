package org.springframework.data.cassandra.test.integration.multipackagescanning.first;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class First {

	@PrimaryKey
	String key;
}
