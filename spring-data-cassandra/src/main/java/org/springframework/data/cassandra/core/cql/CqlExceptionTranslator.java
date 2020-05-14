/*
 * Copyright 2017-2020 the original author or authors.
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

import com.datastax.oss.driver.api.core.DriverException;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.core.mapping.UnsupportedCassandraOperationException;
import org.springframework.lang.Nullable;

/**
 * Strategy interface for translating between {@link RuntimeException driver exceptions} and Spring's data access
 * strategy-agnostic {@link DataAccessException} hierarchy.
 *
 * @author Mark Paluch
 * @see org.springframework.dao.DataAccessException
 * @see 2.0
 */
@FunctionalInterface
public interface CqlExceptionTranslator extends PersistenceExceptionTranslator {

	/**
	 * Translate the given {@link RuntimeException} into a generic {@link DataAccessException}.
	 * <p>
	 * The returned {@link DataAccessException} is supposed to contain the original {@code DriverException} as root cause.
	 * However, client code may not generally rely on this due to {@link DataAccessException}s possibly being caused by
	 * other resource APIs as well. That said, a {@code getRootCause() instanceof DataAccessException} check (and
	 * subsequent cast) is considered reliable when expecting Cassandra-based access to have happened.
	 *
	 * @param task readable text describing the task being attempted.
	 * @param cql CQL query or update that caused the problem (may be {@literal null}).
	 * @param ex the offending {@link DriverException}.
	 * @return the DataAccessException, wrapping the {@link RuntimeException}.
	 * @see org.springframework.dao.DataAccessException#getRootCause()
	 */
	default DataAccessException translate(@Nullable String task, @Nullable String cql, RuntimeException ex) {

		DataAccessException translated = translateExceptionIfPossible(ex);
		return translated == null ? new UnsupportedCassandraOperationException("Cannot translate exception", ex)
				: translated;
	}
}
