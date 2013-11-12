package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.TransientDataAccessException;

public class CassandraTraceRetrievalException extends
		TransientDataAccessException {

	private static final long serialVersionUID = -3163557220324700239L;

	public CassandraTraceRetrievalException(String msg) {
		super(msg);
	}

	public CassandraTraceRetrievalException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
