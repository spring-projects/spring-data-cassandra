package org.springframework.data.cassandra.test.integration.querymethods.bigintparam;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

import java.math.BigInteger;

@Table
public class BigThing {

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private BigInteger number;

	public BigThing() {}

	public BigThing(BigInteger number) {
		setNumber(number);
	}

	public BigInteger getNumber() {
		return number;
	}

	public void setNumber(BigInteger number) {
		this.number = number;
	}
}
