package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.DataAccessResourceFailureException;

public class CassandraNoHostAvailableException extends
		DataAccessResourceFailureException {

	private static final long serialVersionUID = 6299912054261646552L;

	public CassandraNoHostAvailableException(String msg) {
		super(msg);
	}

	public CassandraNoHostAvailableException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
