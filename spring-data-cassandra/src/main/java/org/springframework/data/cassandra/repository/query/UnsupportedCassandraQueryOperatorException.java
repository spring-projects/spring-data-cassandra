package org.springframework.data.cassandra.repository.query;

import org.springframework.dao.InvalidDataAccessApiUsageException;

public class UnsupportedCassandraQueryOperatorException extends InvalidDataAccessApiUsageException {

	public UnsupportedCassandraQueryOperatorException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public UnsupportedCassandraQueryOperatorException(String msg, Throwable cause) {
		super(msg, cause);
		// TODO Auto-generated constructor stub
	}

}
