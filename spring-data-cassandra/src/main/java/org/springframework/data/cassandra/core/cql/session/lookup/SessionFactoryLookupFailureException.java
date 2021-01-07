/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session.lookup;

import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.cassandra.SessionFactory;

/**
 * Exception to be thrown by a {@link SessionFactoryLookup} implementation, indicating that the specified
 * {@link SessionFactory} could not be obtained.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@SuppressWarnings("serial")
public class SessionFactoryLookupFailureException extends NonTransientDataAccessException {

	/**
	 * Create a new {@link SessionFactoryLookupFailureException}.
	 *
	 * @param msg the detail message.
	 */
	public SessionFactoryLookupFailureException(String msg) {
		super(msg);
	}

	/**
	 * Create a new {@link SessionFactoryLookupFailureException}.
	 *
	 * @param msg the detail message.
	 * @param cause the root cause (usually from using a underlying lookup API).
	 */
	public SessionFactoryLookupFailureException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
