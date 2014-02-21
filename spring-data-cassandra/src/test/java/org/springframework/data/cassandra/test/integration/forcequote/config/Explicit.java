package org.springframework.data.cassandra.test.integration.forcequote.config;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table(forceQuote = true, value = Explicit.TABLE_NAME)
public class Explicit {

	public static final String TABLE_NAME = "Xx";

	@PrimaryKey
	String primaryKey;

	String stringValue = UUID.randomUUID().toString();

	public Explicit() {
		this(UUID.randomUUID().toString());
	}

	public Explicit(String primaryKey) {
		setPrimaryKey(primaryKey);
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getStringValue() {
		return stringValue;
	}
}
