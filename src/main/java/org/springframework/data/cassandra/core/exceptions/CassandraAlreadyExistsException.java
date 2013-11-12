package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.NonTransientDataAccessException;

public class CassandraAlreadyExistsException extends
		NonTransientDataAccessException {

	private static final long serialVersionUID = 6032967419751410352L;

	public CassandraAlreadyExistsException(String msg) {
		super(msg);
	}

	public CassandraAlreadyExistsException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
