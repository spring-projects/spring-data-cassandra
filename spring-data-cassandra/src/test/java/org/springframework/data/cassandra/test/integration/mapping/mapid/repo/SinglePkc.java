package org.springframework.data.cassandra.test.integration.mapping.mapid.repo;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class SinglePkc {

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	String key;

	@Column
	String value;

	/**
	 * @deprecated for persistence use only
	 */
	@Deprecated
	@SuppressWarnings("unused")
	private SinglePkc() {}

	public SinglePkc(String key) {
		setKey(key);
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
