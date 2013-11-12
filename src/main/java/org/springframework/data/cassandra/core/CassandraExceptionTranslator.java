/*
 * Copyright 2010-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.core.exceptions.*;

import com.datastax.driver.core.exceptions.*;

/**
 * Simple {@link PersistenceExceptionTranslator} for Cassandra. Convert the
 * given runtime exception to an appropriate exception from the
 * {@code org.springframework.dao} hierarchy. Return {@literal null} if no
 * translation is appropriate: any other exception may have resulted from user
 * code, and should not be translated.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */

public class CassandraExceptionTranslator implements
		PersistenceExceptionTranslator {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#
	 * translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException x) {

		if (!(x instanceof DriverException)) {
			return null;
		}

		if (x instanceof AuthenticationException) {
			return new CassandraAuthenticationException(x.getMessage(), x);
		}
		if (x instanceof DriverInternalError) {
			return new CassandraDriverInternalErrorException(x.getMessage(), x);
		}
		if (x instanceof InvalidTypeException) {
			return new CassandraInvalidTypeException(x.getMessage(), x);
		}
		if (x instanceof NoHostAvailableException) {
			return new CassandraNoHostAvailableException(x.getMessage(), x);
		}
		if (x instanceof ReadTimeoutException) {
			return new CassandraReadTimeoutException(x.getMessage(), x);
		}
		if (x instanceof WriteTimeoutException) {
			return new CassandraWriteTimeoutException(x.getMessage(), x);
		}
		if (x instanceof TruncateException) {
			return new CassandraTruncateException(x.getMessage(), x);
		}
		if (x instanceof UnavailableException) {
			return new CassandraUnavailableException(x.getMessage(), x);
		}
		if (x instanceof AlreadyExistsException) {
			return new CassandraAlreadyExistsException(x.getMessage(), x);
		}
		if (x instanceof InvalidConfigurationInQueryException) {
			return new CassandraInvalidConfigurationInQueryException(
					x.getMessage(), x);
		}
		// this must come after cases for subclasses
		if (x instanceof InvalidQueryException) {
			return new CassandraInvalidQueryException(x.getMessage(), x);
		}
		if (x instanceof SyntaxError) {
			return new CassandraSyntaxErrorException(x.getMessage(), x);
		}
		if (x instanceof UnauthorizedException) {
			return new CassandraUnauthorizedException(x.getMessage(), x);
		}
		if (x instanceof TraceRetrievalException) {
			return new CassandraTraceRetrievalException(x.getMessage(), x);
		}

		// unknown or unhandled exception
		return new CassandraUncategorizedException(x.getMessage(), x);
	}
}
