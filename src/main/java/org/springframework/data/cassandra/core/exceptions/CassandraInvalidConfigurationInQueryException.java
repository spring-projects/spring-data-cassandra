package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.InvalidDataAccessApiUsageException;

public class CassandraInvalidConfigurationInQueryException extends
		InvalidDataAccessApiUsageException {

	private static final long serialVersionUID = 4594321191806182918L;

	public CassandraInvalidConfigurationInQueryException(String msg) {
		super(msg);
	}

	public CassandraInvalidConfigurationInQueryException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
