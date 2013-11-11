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
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 * Simple {@link PersistenceExceptionTranslator} for Cassandra. Convert the given runtime exception to an appropriate
 * exception from the {@code org.springframework.dao} hierarchy. Return {@literal null} if no translation is
 * appropriate: any other exception may have resulted from user code, and should not be translated.
 * 
 * @author Alex Shvid
 */

public class CassandraExceptionTranslator implements PersistenceExceptionTranslator {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {

		// Check for well-known Cassandra subclasses.
		
		if (ex instanceof InvalidQueryException) {
			return new DataAccessResourceFailureException(ex.getMessage(), ex);
		}
		
		// If we get here, we have an exception that resulted from user code,
		// rather than the persistence provider, so we return null to indicate
		// that translation should not occur.
		return null;
	}
	
}
