package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.InvalidDataAccessApiUsageException;

public class CassandraInvalidQueryException extends
		InvalidDataAccessApiUsageException {

	private static final long serialVersionUID = 4594321191806182918L;

	public CassandraInvalidQueryException(String msg) {
		super(msg);
	}

	public CassandraInvalidQueryException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
