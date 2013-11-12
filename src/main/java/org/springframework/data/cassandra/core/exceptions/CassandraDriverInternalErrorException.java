package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.DataAccessResourceFailureException;

public class CassandraDriverInternalErrorException extends
		DataAccessResourceFailureException {

	private static final long serialVersionUID = 433061676465346338L;

	public CassandraDriverInternalErrorException(String msg) {
		super(msg);
	}

	public CassandraDriverInternalErrorException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
