package org.springframework.data.cassandra.exception;

import org.springframework.dao.InvalidDataAccessApiUsageException;

public class UnsupportedCassandraOperationException extends InvalidDataAccessApiUsageException {

	private static final long serialVersionUID = 4921001859094231277L;

	public UnsupportedCassandraOperationException(String msg) {
		super(msg);
	}

	public UnsupportedCassandraOperationException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
