package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table(forceQuote = true, value = Explicit.TABLE_NAME)
public class Explicit {

	public static final String TABLE_NAME = "JavaExplicitTable";
	public static final String STRING_VALUE_COLUMN_NAME = "JavaExplicitStringValue";

	@PrimaryKey
	ExplicitKey primaryKey;

	@Column(value = STRING_VALUE_COLUMN_NAME, forceQuote = true)
	String stringValue = UUID.randomUUID().toString();

	@SuppressWarnings("unused")
	private Explicit() {}

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

	public void setStringValue(String stringValue) {
		this.stringValue = stringValue;
	}
}
