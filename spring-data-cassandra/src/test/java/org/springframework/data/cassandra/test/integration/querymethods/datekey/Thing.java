package org.springframework.data.cassandra.test.integration.querymethods.datekey;

import java.util.Date;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.util.Assert;

@Table
public class Thing {

	@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private Date date;

	protected Thing() {}

	public Thing(Date date) {
		setDate(date);
	}

	public Date getDate() {
		return new Date(date.getTime());
	}

	public void setDate(Date date) {
		Assert.notNull(date);
		this.date = new Date(date.getTime());
	}
}
