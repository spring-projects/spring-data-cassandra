package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.PermissionDeniedDataAccessException;

public class CassandraUnauthorizedException extends
		PermissionDeniedDataAccessException {

	private static final long serialVersionUID = 4618185356687726647L;

	public CassandraUnauthorizedException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
