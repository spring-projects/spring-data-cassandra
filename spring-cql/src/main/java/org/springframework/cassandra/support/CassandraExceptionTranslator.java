/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.support;

import org.springframework.cassandra.core.support.CQLExceptionTranslator;
import org.springframework.cassandra.support.exception.CassandraAuthenticationException;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.cassandra.support.exception.CassandraInsufficientReplicasAvailableException;
import org.springframework.cassandra.support.exception.CassandraInternalException;
import org.springframework.cassandra.support.exception.CassandraInvalidConfigurationInQueryException;
import org.springframework.cassandra.support.exception.CassandraInvalidQueryException;
import org.springframework.cassandra.support.exception.CassandraKeyspaceExistsException;
import org.springframework.cassandra.support.exception.CassandraQuerySyntaxException;
import org.springframework.cassandra.support.exception.CassandraReadTimeoutException;
import org.springframework.cassandra.support.exception.CassandraTableExistsException;
import org.springframework.cassandra.support.exception.CassandraTraceRetrievalException;
import org.springframework.cassandra.support.exception.CassandraTruncateException;
import org.springframework.cassandra.support.exception.CassandraTypeMismatchException;
import org.springframework.cassandra.support.exception.CassandraUnauthorizedException;
import org.springframework.cassandra.support.exception.CassandraUncategorizedException;
import org.springframework.cassandra.support.exception.CassandraWriteTimeoutException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.TraceRetrievalException;
import com.datastax.driver.core.exceptions.TruncateException;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

/**
 * Simple {@link PersistenceExceptionTranslator} for Cassandra.
 * <p>
 * Convert the given runtime exception to an appropriate exception from the {@code org.springframework.dao} hierarchy.
 * Return {@literal null} if no translation is appropriate: any other exception may have resulted from user code, and
 * should not be translated.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraExceptionTranslator implements CQLExceptionTranslator {

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {

		if (ex instanceof DataAccessException) {
			return (DataAccessException) ex;
		}

		if (!(ex instanceof DriverException)) {
			return null;
		}

		return translate(null, null, (DriverException) ex);
	}

	@Override
	public DataAccessException translate(String task, String cql, DriverException ex) {

		String message = buildMessage(task, cql, ex);

		// Remember: subclasses must come before superclasses, otherwise the
		// superclass would match before the subclass!

		if (ex instanceof AuthenticationException) {
			return new CassandraAuthenticationException(((AuthenticationException) ex).getHost(), message, ex);
		}
		if (ex instanceof DriverInternalError) {
			return new CassandraInternalException(message, ex);
		}
		if (ex instanceof InvalidTypeException) {
			return new CassandraTypeMismatchException(message, ex);
		}
		if (ex instanceof NoHostAvailableException) {
			return new CassandraConnectionFailureException(((NoHostAvailableException) ex).getErrors(), message, ex);
		}
		if (ex instanceof ReadTimeoutException) {
			return new CassandraReadTimeoutException(((ReadTimeoutException) ex).wasDataRetrieved(), message, ex);
		}
		if (ex instanceof WriteTimeoutException) {
			WriteType writeType = ((WriteTimeoutException) ex).getWriteType();
			return new CassandraWriteTimeoutException(writeType == null ? null : writeType.name(), message, ex);
		}
		if (ex instanceof TruncateException) {
			return new CassandraTruncateException(message, ex);
		}
		if (ex instanceof UnavailableException) {
			UnavailableException ux = (UnavailableException) ex;
			return new CassandraInsufficientReplicasAvailableException(ux.getRequiredReplicas(), ux.getAliveReplicas(),
					message, ex);
		}
		if (ex instanceof AlreadyExistsException) {
			AlreadyExistsException aex = (AlreadyExistsException) ex;

			return aex.wasTableCreation() ? new CassandraTableExistsException(aex.getTable(), message, ex)
					: new CassandraKeyspaceExistsException(aex.getKeyspace(), message, ex);
		}
		if (ex instanceof InvalidConfigurationInQueryException) {
			return new CassandraInvalidConfigurationInQueryException(message, ex);
		}
		if (ex instanceof InvalidQueryException) {
			return new CassandraInvalidQueryException(message, ex);
		}
		if (ex instanceof SyntaxError) {
			return new CassandraQuerySyntaxException(message, ex);
		}
		if (ex instanceof UnauthorizedException) {
			return new CassandraUnauthorizedException(message, ex);
		}
		if (ex instanceof TraceRetrievalException) {
			return new CassandraTraceRetrievalException(message, ex);
		}

		// unknown or unhandled exception
		return new CassandraUncategorizedException(message, ex);
	}

	/**
	 * Build a message {@code String} for the given {@link DriverException}.
	 * <p>
	 * To be called by translator subclasses when creating an instance of a generic
	 * {@link org.springframework.dao.DataAccessException} class.
	 * 
	 * @param task readable text describing the task being attempted
	 * @param cql the CQL statement that caused the problem (may be {@code null})
	 * @param ex the offending {@code DriverException}
	 * @return the message {@code String} to use
	 */
	protected String buildMessage(String task, String cql, DriverException ex) {

		if (StringUtils.hasText(task) || StringUtils.hasText(cql)) {
			return task + "; CQL [" + cql + "]; " + ex.getMessage();
		}

		return ex.getMessage();
	}
}
