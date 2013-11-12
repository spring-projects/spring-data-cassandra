package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.QueryTimeoutException;

public class CassandraReadTimeoutException extends QueryTimeoutException {

	private static final long serialVersionUID = -787022307935203387L;

	public CassandraReadTimeoutException(String msg) {
		super(msg);
	}

	public CassandraReadTimeoutException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
