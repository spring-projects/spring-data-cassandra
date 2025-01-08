/*
 * Copyright 2013-2025 the original author or authors.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.dao.DataAccessResourceFailureException;

import com.datastax.oss.driver.api.core.metadata.Node;

/**
 * Spring data access exception for Cassandra when no host is available.
 *
 * @author Matthew T. Adams
 */
public class CassandraConnectionFailureException extends DataAccessResourceFailureException {

	@Serial private static final long serialVersionUID = 6299912054261646552L;

	private final Map<Node, Throwable> messagesByHost;

	/**
	 * Constructor for {@link CassandraConnectionFailureException}.
	 *
	 * @param map the detail failures for each node.
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public CassandraConnectionFailureException(Map<Node, Throwable> map, String msg, Throwable cause) {
		super(msg, cause);
		this.messagesByHost = new HashMap<>(map);
	}

	public Map<Node, Throwable> getMessagesByHost() {
		return Collections.unmodifiableMap(messagesByHost);
	}
}
