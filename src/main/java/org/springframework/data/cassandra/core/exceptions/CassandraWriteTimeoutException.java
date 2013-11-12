package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.QueryTimeoutException;

public class CassandraWriteTimeoutException extends QueryTimeoutException {

	private static final long serialVersionUID = -4374826375213670718L;

	public CassandraWriteTimeoutException(String msg) {
		super(msg);
	}

	public CassandraWriteTimeoutException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
