package org.springframework.data.cassandra;

import org.springframework.dao.QueryTimeoutException;

/**
 * This exception is thrown when driver have timed out during any interation with cassandra coordinator node
 *
 * @author Mikhail Polivakha
 */
public class CassandraDriverTimeOutException extends QueryTimeoutException {

    public CassandraDriverTimeOutException(String message, Throwable cause) {
        super(message, cause);
    }
}
