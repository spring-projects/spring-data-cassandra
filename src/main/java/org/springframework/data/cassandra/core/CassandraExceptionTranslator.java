/*
 * Copyright 2013 the original author or authors.
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

import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.datastax.driver.core.exceptions.*;

/**
 * Simple {@link PersistenceExceptionTranslator} for Cassandra. Convert the given runtime exception to an appropriate
 * exception from the {@code org.springframework.dao} hierarchy. Return {@literal null} if no translation is
 * appropriate: any other exception may have resulted from user code, and should not be translated.
 * 
 * @author David Webb
 */
public class CassandraExceptionTranslator implements PersistenceExceptionTranslator {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException rte) {
		
		/*
		 * Since all Cassandra Exceptions extend Exception, cast so we can do the comparison
		 */
		Exception ex = (Exception)rte;

		if (ex instanceof AuthenticationException) {
			return new PermissionDeniedDataAccessException(ex.getMessage(), ex);
		}

//		if (ex instanceof BadConfigurationException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof BadRequestException) {
//			return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof ConnectionAbortedException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof ConnectionException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof HostDownException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof InterruptedOperationException) {
//			return new TransientDataAccessResourceException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof IsDeadConnectionException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof IsRetryableException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof IsTimeoutException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof MaxConnsPerHostReachedException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof NoAvailableHostsException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof NotFoundException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof OperationException) {
//			return new TransientDataAccessResourceException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof OperationTimeoutException) {
//			return new QueryTimeoutException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof PoolTimeoutException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof SchemaDisagreementException) {
//			return new TransientDataAccessResourceException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof SerializationException) {
//			return new CannotSerializeTransactionException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof ThriftStateException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof ThrottledException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof TimeoutException) {
//			return new QueryTimeoutException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof TokenRangeOfflineException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof TransportException) {
//			return new DataAccessResourceFailureException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof UnknownException) {
//			return new TransientDataAccessResourceException(ex.getMessage(), ex);
//		}
//
//		if (ex instanceof WalException) {
//			return new TransientDataAccessResourceException(ex.getMessage(), ex);
//		}


		/*
		 * Indicate that no translation is possible.
		 */
		return null;
	}
}
