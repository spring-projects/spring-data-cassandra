package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.TypeMismatchDataAccessException;

public class CassandraInvalidTypeException extends
		TypeMismatchDataAccessException {

	private static final long serialVersionUID = -7420058975444905629L;

	public CassandraInvalidTypeException(String msg) {
		super(msg);
	}

	public CassandraInvalidTypeException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
