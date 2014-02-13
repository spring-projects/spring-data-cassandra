package org.springframework.data.cassandra.test.integration.multipackagescanning.second;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class Second {

	@PrimaryKey
	String key;
}
