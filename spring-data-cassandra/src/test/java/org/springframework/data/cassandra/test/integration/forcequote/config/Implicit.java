package org.springframework.data.cassandra.test.integration.forcequote.config;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table(forceQuote = true)
public class Implicit {

	@PrimaryKey
	String key;

	public Implicit() {
		this(UUID.randomUUID().toString());
	}

	public Implicit(String key) {
		setKey(key);
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}
