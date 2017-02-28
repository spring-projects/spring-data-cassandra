/*
 * Copyright 2013-2017 the original author or authors.
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

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.cassandra.support.exception.*;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.*;

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
public class CassandraExceptionTranslator implements PersistenceExceptionTranslator {

	private static final Set<String> CONNECTION_FAILURE_TYPES = new HashSet<String>(
			Arrays.asList("NoHostAvailableException", "ConnectionException", "OperationTimedOutException",
					"TransportException", "BusyConnectionException", "BusyPoolException"));

	private static final Set<String> RESOURCE_FAILURE_TYPES = new HashSet<String>(
			Arrays.asList("ReadFailureException", "WriteFailureException", "FunctionExecutionException"));

	/* (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException exception) {

		if (exception instanceof DataAccessException) {
			return (DataAccessException) exception;
		}

		if (!(exception instanceof DriverException)) {
			return null;
		}

		// Remember: subclasses must come before superclasses, otherwise the
		// superclass would match before the subclass!

		if (exception instanceof AuthenticationException) {
			return new CassandraAuthenticationException(((AuthenticationException) exception).getHost(),
					exception.getMessage(), exception);
		}

		if (exception instanceof DriverInternalError) {
			return new CassandraInternalException(exception.getMessage(), exception);
		}

		if (exception instanceof InvalidTypeException) {
			return new CassandraTypeMismatchException(exception.getMessage(), exception);
		}

		if (exception instanceof ReadTimeoutException) {
			return new CassandraReadTimeoutException(((ReadTimeoutException) exception).wasDataRetrieved(),
					exception.getMessage(), exception);
		}

		if (exception instanceof WriteTimeoutException) {

			WriteType writeType = ((WriteTimeoutException) exception).getWriteType();
			return new CassandraWriteTimeoutException(writeType == null ? null : writeType.name(), exception.getMessage(),
					exception);
		}

		if (exception instanceof TruncateException) {
			return new CassandraTruncateException(exception.getMessage(), exception);
		}

		if (exception instanceof UnavailableException) {

			UnavailableException ux = (UnavailableException) exception;
			return new CassandraInsufficientReplicasAvailableException(ux.getRequiredReplicas(), ux.getAliveReplicas(),
					exception.getMessage(), exception);
		}

		if (exception instanceof OverloadedException || exception instanceof BootstrappingException) {
			return new TransientDataAccessResourceException(exception.getMessage(), exception);
		}

		if (exception instanceof AlreadyExistsException) {

			AlreadyExistsException aex = (AlreadyExistsException) exception;

			return aex.wasTableCreation()
					? new CassandraTableExistsException(aex.getTable(), exception.getMessage(), exception)
					: new CassandraKeyspaceExistsException(aex.getKeyspace(), exception.getMessage(), exception);
		}

		if (exception instanceof InvalidConfigurationInQueryException) {
			return new CassandraInvalidConfigurationInQueryException(exception.getMessage(), exception);
		}

		if (exception instanceof InvalidQueryException) {
			return new CassandraInvalidQueryException(exception.getMessage(), exception);
		}

		if (exception instanceof SyntaxError) {
			return new CassandraQuerySyntaxException(exception.getMessage(), exception);
		}

		if (exception instanceof UnauthorizedException) {
			return new CassandraUnauthorizedException(exception.getMessage(), exception);
		}

		if (exception instanceof TraceRetrievalException) {
			return new CassandraTraceRetrievalException(exception.getMessage(), exception);
		}

		if (exception instanceof NoHostAvailableException) {
			return new CassandraConnectionFailureException(((NoHostAvailableException) exception).getErrors(),
					exception.getMessage(), exception);
		}

		String exceptionType = ClassUtils.getShortName(ClassUtils.getUserClass(exception.getClass()));

		if (CONNECTION_FAILURE_TYPES.contains(exceptionType)) {

			Map<InetSocketAddress, Throwable> errorMap = Collections.emptyMap();

			if (exception instanceof CoordinatorException) {
				CoordinatorException cx = (CoordinatorException) exception;
				errorMap = Collections.<InetSocketAddress, Throwable> singletonMap(cx.getAddress(), exception);
			}

			return new CassandraConnectionFailureException(errorMap, exception.getMessage(), exception);
		}

		if (RESOURCE_FAILURE_TYPES.contains(exceptionType)) {
			return new DataAccessResourceFailureException(exception.getMessage(), exception);
		}

		// unknown or unhandled exception
		return new CassandraUncategorizedException(exception.getMessage(), exception);
	}
}
