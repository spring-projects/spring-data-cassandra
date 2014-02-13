package org.springframework.data.cassandra.test.integration.multipackagescanning;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class Top {

	@PrimaryKey
	String key;
}
