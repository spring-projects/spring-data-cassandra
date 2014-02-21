package org.springframework.data.cassandra.test.integration.forcequote.config;

import java.util.UUID;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class ImplicitProperties {

	@PrimaryKey(forceQuote = true)
	String primaryKey;

	@Column(forceQuote = true)
	String stringValue = UUID.randomUUID().toString();

	public ImplicitProperties() {
		this(UUID.randomUUID().toString());
	}

	public ImplicitProperties(String primaryKey) {
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
