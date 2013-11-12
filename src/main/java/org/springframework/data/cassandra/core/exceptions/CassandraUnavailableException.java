package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.ConcurrencyFailureException;

public class CassandraUnavailableException extends ConcurrencyFailureException {

	private static final long serialVersionUID = 6415130674604814905L;

	public CassandraUnavailableException(String msg) {
		super(msg);
	}

	public CassandraUnavailableException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
