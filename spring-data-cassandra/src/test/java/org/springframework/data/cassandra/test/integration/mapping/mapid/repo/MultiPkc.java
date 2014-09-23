package org.springframework.data.cassandra.test.integration.mapping.mapid.repo;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

@Table
public class MultiPkc {

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	String key0;

	@PrimaryKeyColumn(ordinal = 1)
	String key1;

	@Column
	String value;

	/**
	 * @deprecated for persistence use only
	 */
	@Deprecated
	@SuppressWarnings("unused")
	private MultiPkc() {}

	public MultiPkc(String key0, String key1) {
		setKey0(key0);
		setKey1(key1);
	}

	public String getKey0() {
		return key0;
	}

	public void setKey0(String key0) {
		this.key0 = key0;
	}

	public String getKey1() {
		return key1;
	}

	public void setKey1(String key1) {
		this.key1 = key1;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
