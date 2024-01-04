package org.springframework.data.cassandra;

import org.springframework.dao.NonTransientDataAccessException;

/**
 * The exception that is thrown in case cassandra schema in the particular keyspace does not match
 * the configuration of the entities inside application.
 *
 * @author Mikhail Polivakha
 */
public class CassandraSchemaValidationException extends NonTransientDataAccessException {

    public CassandraSchemaValidationException(String message) {
        super(message);
    }
}
