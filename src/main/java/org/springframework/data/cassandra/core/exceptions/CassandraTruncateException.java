package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.TransientDataAccessException;

public class CassandraTruncateException extends TransientDataAccessException {

	private static final long serialVersionUID = 5730642491362430311L;

	public CassandraTruncateException(String msg) {
		super(msg);
	}

	public CassandraTruncateException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
