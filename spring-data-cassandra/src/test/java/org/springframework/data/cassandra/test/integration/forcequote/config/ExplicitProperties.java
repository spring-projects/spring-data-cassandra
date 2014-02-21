package org.springframework.data.cassandra.test.integration.forcequote.config;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class ExplicitProperties {

	public static final String EXPLICIT_PRIMARY_KEY = "ThePrimaryKey";
	public static final String EXPLICIT_STRING_VALUE = "TheStringValue";

	@PrimaryKey(forceQuote = true, value = EXPLICIT_PRIMARY_KEY)
	String primaryKey;

	@Column(forceQuote = true, value = EXPLICIT_STRING_VALUE)
	String stringValue = UUID.randomUUID().toString();

	public ExplicitProperties() {
		this(UUID.randomUUID().toString());
	}

	public ExplicitProperties(String primaryKey) {
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

	public void setStringValue(String stringy) {
		this.stringValue = stringy;
	}
}
