package org.springframework.data.cassandra;

import org.springframework.dao.NonTransientDataAccessException;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Exception that is thrown in case {@link CqlSession} has no active keyspace set. This should not
 * typically happen. This exception means some misconfiguration within framework.
 *
 * @author Mikhail Polivakha
 */
public class CassandraNoActiveKeyspaceSetForCqlSessionException extends NonTransientDataAccessException {

    public CassandraNoActiveKeyspaceSetForCqlSessionException() {
        super("There is no active keyspace set for CqlSession");
    }
}
