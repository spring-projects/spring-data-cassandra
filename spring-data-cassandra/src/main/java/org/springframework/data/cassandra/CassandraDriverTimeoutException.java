package org.springframework.data.cassandra;

import org.springframework.dao.QueryTimeoutException;

/**
 * This exception is thrown when driver has timed out during any interaction with the Cassandra coordinator node.
 *
 * @author Mikhail Polivakha
 * @since 4.2
 */
public class CassandraDriverTimeoutException extends QueryTimeoutException {

	/**
	 * Constructor for {@link CassandraDriverTimeoutException}.
	 *
	 * @param message the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public CassandraDriverTimeoutException(String message, Throwable cause) {
		super(message, cause);
	}
}
