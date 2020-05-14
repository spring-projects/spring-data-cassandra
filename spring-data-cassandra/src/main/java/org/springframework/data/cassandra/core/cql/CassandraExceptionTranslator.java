/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.*;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.*;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Simple {@link PersistenceExceptionTranslator} for Cassandra.
 * <p>
 * Convert the given runtime exception to an appropriate exception from the {@code org.springframework.dao} hierarchy.
 * Preserves exception if it's already a {@link DataAccessException} and ignores non {@link DriverException}s returning
 * {@literal null}. Falls back to {@link CassandraUncategorizedException} in case there's no mapping to a more detailed
 * exception.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
public class CassandraExceptionTranslator implements CqlExceptionTranslator {

	private static final Set<String> CONNECTION_FAILURE_TYPES = new HashSet<>(
			Arrays.asList("NoHostAvailableException", "ConnectionException", "OperationTimedOutException",
					"TransportException", "BusyConnectionException", "BusyPoolException"));

	private static final Set<String> RESOURCE_FAILURE_TYPES = new HashSet<>(
			Arrays.asList("ReadFailureException", "WriteFailureException", "FunctionExecutionException"));

	/* (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	@Override
	@Nullable
	public DataAccessException translateExceptionIfPossible(RuntimeException exception) {

		if (exception instanceof DataAccessException) {
			return (DataAccessException) exception;
		}

		return translate(null, null, exception);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.cql.CQLExceptionTranslator#translate(java.lang.String, java.lang.String, com.datastax.oss.driver.api.core.DriverException)
	 */
	@Override
	public DataAccessException translate(@Nullable String task, @Nullable String cql, RuntimeException exception) {

		String message = buildMessage(task, cql, exception);

		// Remember: subclasses must come before superclasses, otherwise the
		// superclass would match before the subclass!

		if (exception instanceof AuthenticationException) {
			return new CassandraAuthenticationException(((AuthenticationException) exception).getEndPoint(), message,
					exception);
		}

		if (exception instanceof ReadTimeoutException) {
			return new CassandraReadTimeoutException(((ReadTimeoutException) exception).wasDataPresent(), message, exception);
		}

		if (exception instanceof WriteTimeoutException) {

			WriteType writeType = ((WriteTimeoutException) exception).getWriteType();
			return new CassandraWriteTimeoutException(writeType == null ? null : writeType.name(), message, exception);
		}

		if (exception instanceof TruncateException) {
			return new CassandraTruncateException(message, exception);
		}

		if (exception instanceof UnavailableException) {

			UnavailableException ux = (UnavailableException) exception;
			return new CassandraInsufficientReplicasAvailableException(ux.getRequired(), ux.getAlive(), message, exception);
		}

		if (exception instanceof OverloadedException || exception instanceof BootstrappingException) {
			return new TransientDataAccessResourceException(message, exception);
		}
		if (exception instanceof AlreadyExistsException) {

			AlreadyExistsException aex = (AlreadyExistsException) exception;
			return new CassandraSchemaElementExistsException(aex.getMessage(), aex);
		}

		if (exception instanceof InvalidConfigurationInQueryException) {
			return new CassandraInvalidConfigurationInQueryException(message, exception);
		}

		if (exception instanceof InvalidQueryException) {
			return new CassandraInvalidQueryException(message, exception);
		}

		if (exception instanceof SyntaxError) {
			return new CassandraQuerySyntaxException(message, exception);
		}

		if (exception instanceof UnauthorizedException) {
			return new CassandraUnauthorizedException(message, exception);
		}

		if (exception instanceof AllNodesFailedException) {
			return new CassandraConnectionFailureException(((AllNodesFailedException) exception).getErrors(), message,
					exception);
		}

		String exceptionType = ClassUtils.getShortName(ClassUtils.getUserClass(exception.getClass()));

		if (CONNECTION_FAILURE_TYPES.contains(exceptionType)) {

			Map<Node, Throwable> errorMap = Collections.emptyMap();

			if (exception instanceof CoordinatorException) {
				CoordinatorException cx = (CoordinatorException) exception;
				errorMap = Collections.singletonMap(cx.getCoordinator(), exception);
			}

			return new CassandraConnectionFailureException(errorMap, message, exception);
		}

		if (RESOURCE_FAILURE_TYPES.contains(exceptionType)) {
			return new DataAccessResourceFailureException(message, exception);
		}

		// unknown or unhandled exception
		return new CassandraUncategorizedException(message, exception);
	}

	/**
	 * Build a message {@code String} for the given {@link DriverException}.
	 * <p>
	 * To be called by translator subclasses when creating an instance of a generic
	 * {@link org.springframework.dao.DataAccessException} class.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql the CQL statement that caused the problem (may be {@literal null})
	 * @param ex the offending {@code DriverException}
	 * @return the message {@code String} to use
	 */
	protected String buildMessage(@Nullable String task, @Nullable String cql, RuntimeException ex) {

		if (StringUtils.hasText(task) || StringUtils.hasText(cql)) {
			return task + "; CQL [" + cql + "]; " + ex.getMessage();
		}

		return ex.getMessage();
	}
}
