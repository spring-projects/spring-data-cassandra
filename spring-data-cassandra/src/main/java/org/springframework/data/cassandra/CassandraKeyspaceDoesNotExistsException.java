package org.springframework.data.cassandra;

import org.springframework.dao.NonTransientDataAccessException;

/**
 * The exception to be thrown when keyspace that expected to be present is missing in the cluster
 *
 * @author Mikhail Polivakha
 */
public class CassandraKeyspaceDoesNotExistsException extends NonTransientDataAccessException {

    public CassandraKeyspaceDoesNotExistsException(String keyspace) {
        super("Keyspace %s does not exists in the cluster".formatted(keyspace));
    }
}
