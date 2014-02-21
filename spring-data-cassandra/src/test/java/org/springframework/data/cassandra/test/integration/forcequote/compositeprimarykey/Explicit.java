package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table(forceQuote = true)
public class Explicit {

	@PrimaryKey
	ExplicitKey primaryKey;

	String stringValue = UUID.randomUUID().toString();

	@SuppressWarnings("unused")
	private Explicit() {
	}

	public Explicit(ExplicitKey primaryKey) {
		setPrimaryKey(primaryKey);
	}

	public ExplicitKey getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(ExplicitKey primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getStringValue() {
		return stringValue;
	}
}
