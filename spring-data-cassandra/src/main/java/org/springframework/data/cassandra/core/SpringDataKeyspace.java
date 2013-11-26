package org.springframework.data.cassandra.core;

import org.springframework.cassandra.core.Keyspace;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * @deprecated This needs more thought.
 */
@Deprecated
public class SpringDataKeyspace extends Keyspace {

	private CassandraConverter converter;

	public SpringDataKeyspace(String keyspace, Session session, CassandraConverter converter) {
		super(keyspace, session);
		setCassandraConverter(converter);
	}

	public CassandraConverter getCassandraConverter() {
		return converter;
	}

	private void setCassandraConverter(CassandraConverter converter) {
		Assert.notNull(converter);
		this.converter = converter;
	}
}
