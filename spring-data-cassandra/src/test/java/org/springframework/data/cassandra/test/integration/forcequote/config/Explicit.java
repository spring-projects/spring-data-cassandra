package org.springframework.data.cassandra.test.integration.forcequote.config;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table(forceQuote = true, value = Explicit.TABLE_NAME)
public class Explicit {

	public static final String TABLE_NAME = "Xx";

	@PrimaryKey
	String key;

	public Explicit() {
		this(UUID.randomUUID().toString());
	}

	public Explicit(String key) {
		setKey(key);
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
}
