/*
 * Copyright 2013-present the original author or authors.
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
package org.springframework.data.cassandra;

import java.io.Serial;

import org.springframework.dao.QueryTimeoutException;

/**
 * Cassandra-specific exception for a server-side timeout during a read query.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraReadTimeoutException extends QueryTimeoutException {

	@Serial private static final long serialVersionUID = -787022307935203387L;

	private final boolean wasDataPresent;

	/**
	 * Constructor for {@link CassandraReadTimeoutException}.
	 *
	 * @param wasDataPresent whether the actual data was amongst the received replica responses.
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public CassandraReadTimeoutException(boolean wasDataPresent, String msg, Throwable cause) {
		super(msg, cause);
		this.wasDataPresent = wasDataPresent;
	}

	/**
	 * @return whether the actual data was amongst the received replica responses.
	 * @deprecated since 3.0, use {@link #wasDataPresent()}.
	 */
	@Deprecated(forRemoval = true)
	public boolean getWasDataReceived() {
		return wasDataPresent();
	}

	/**
	 * @return whether the actual data was amongst the received replica responses.
	 * @since 3.0
	 */
	public boolean wasDataPresent() {
		return wasDataPresent;
	}
}
