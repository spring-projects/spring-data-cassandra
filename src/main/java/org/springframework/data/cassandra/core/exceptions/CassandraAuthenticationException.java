package org.springframework.data.cassandra.core.exceptions;

import org.springframework.dao.PermissionDeniedDataAccessException;

public class CassandraAuthenticationException extends
		PermissionDeniedDataAccessException {

	private static final long serialVersionUID = 8556304586797273927L;

	public CassandraAuthenticationException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
