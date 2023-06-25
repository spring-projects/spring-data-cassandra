/*
 * Copyright 2013-2023 the original author or authors.
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

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.CassandraAuthenticationException;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.data.cassandra.CassandraInsufficientReplicasAvailableException;
import org.springframework.data.cassandra.CassandraInvalidConfigurationInQueryException;
import org.springframework.data.cassandra.CassandraInvalidQueryException;
import org.springframework.data.cassandra.CassandraQuerySyntaxException;
import org.springframework.data.cassandra.CassandraReadTimeoutException;
import org.springframework.data.cassandra.CassandraSchemaElementExistsException;
import org.springframework.data.cassandra.CassandraTruncateException;
import org.springframework.data.cassandra.CassandraUnauthorizedException;
import org.springframework.data.cassandra.CassandraUncategorizedException;
import org.springframework.data.cassandra.CassandraWriteTimeoutException;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.InvalidConfigurationInQueryException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;

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
public class CassandraExceptionTranslator implements CqlExceptionTranslator {

	private static final Set<String> CONNECTION_FAILURE_TYPES = new HashSet<>(
			Arrays.asList("NoHostAvailableException", "ConnectionException", "OperationTimedOutException",
					"TransportException", "BusyConnectionException", "BusyPoolException"));

	private static final Set<String> RESOURCE_FAILURE_TYPES = new HashSet<>(
			Arrays.asList("ReadFailureException", "WriteFailureException", "FunctionExecutionException"));

	@Override
	@Nullable
	public DataAccessException translateExceptionIfPossible(RuntimeException exception) {

		if (exception instanceof DataAccessException) {
			return (DataAccessException) exception;
		}

		return translate(null, null, exception);
	}

	@Override
	public DataAccessException translate(@Nullable String task, @Nullable String cql, RuntimeException exception) {

		String message = buildMessage(task, cql, exception);

		// Remember: subclasses must come before superclasses, otherwise the
		// superclass would match before the subclass!

		if (exception instanceof AuthenticationException) {
			return new CassandraAuthenticationException(((AuthenticationException) exception).getEndPoint(), message, exception);
		}

		if (exception instanceof ReadTimeoutException) {
			return new CassandraReadTimeoutException(((ReadTimeoutException) exception).wasDataPresent(), message, exception);
		}

		if (exception instanceof WriteTimeoutException writeTimeoutException) {

			WriteType writeType = writeTimeoutException.getWriteType();
			return new CassandraWriteTimeoutException(writeType.name(), message, exception);
		}

		if (exception instanceof TruncateException) {
			return new CassandraTruncateException(message, exception);
		}

		if (exception instanceof UnavailableException unavailableException) {
			return new CassandraInsufficientReplicasAvailableException(unavailableException.getRequired(), unavailableException.getAlive(), message, exception);
		}

		if (exception instanceof OverloadedException || exception instanceof BootstrappingException) {
			return new TransientDataAccessResourceException(message, exception);
		}

		if (exception instanceof AlreadyExistsException alreadyExistsException) {
			return new CassandraSchemaElementExistsException(alreadyExistsException.getMessage(), alreadyExistsException);
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

		if (exception instanceof AllNodesFailedException allNodesFailedException) {
			return new CassandraConnectionFailureException(message, allNodesFailedException.getAllErrors(), exception);
		}

		String exceptionType = ClassUtils.getShortName(ClassUtils.getUserClass(exception.getClass()));

		if (CONNECTION_FAILURE_TYPES.contains(exceptionType)) {

			Map<Node, Throwable> errorMap = Collections.emptyMap();

			if (exception instanceof CoordinatorException cx) {
				errorMap = Collections.singletonMap(cx.getCoordinator(), exception);
			}

			return new CassandraConnectionFailureException(errorMap, message, exception);
		}

		if (RESOURCE_FAILURE_TYPES.contains(exceptionType)) {
			return new DataAccessResourceFailureException(message, exception);
		}

		if (exception instanceof DriverException
				|| (exception.getClass().getName().startsWith("com.datastax.oss.driver"))) {
			// unknown or unhandled exception
			return new CassandraUncategorizedException(message, exception);
		}

		return null;
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
