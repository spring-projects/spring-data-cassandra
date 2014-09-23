package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table(forceQuote = true)
public class Implicit {

	@PrimaryKey
	ImplicitKey primaryKey;

	String stringValue = UUID.randomUUID().toString();

	@SuppressWarnings("unused")
	private Implicit() {}

	public Implicit(ImplicitKey primaryKey) {
		setPrimaryKey(primaryKey);
	}

	public ImplicitKey getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(ImplicitKey primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getStringValue() {
		return stringValue;
	}

	public void setStringValue(String stringValue) {
		this.stringValue = stringValue;
	}
}
