package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.InvalidDataAccessApiUsageException;

public class CassandraSyntaxErrorException extends InvalidDataAccessApiUsageException {

	private static final long serialVersionUID = 4398474399882434154L;

	public CassandraSyntaxErrorException(String msg) {
		super(msg);
	}

	public CassandraSyntaxErrorException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
