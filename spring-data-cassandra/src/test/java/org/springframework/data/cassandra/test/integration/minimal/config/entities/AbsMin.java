package org.springframework.data.cassandra.test.integration.minimal.config.entities;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class AbsMin {

	@PrimaryKey
	String key;

}
