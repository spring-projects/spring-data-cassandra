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

/**
 * Spring data access exception for Cassandra when a keyspace being created already exists.
 *
 * @author Matthew T. Adams
 */
public class CassandraKeyspaceExistsException extends CassandraSchemaElementExistsException {

	@Serial private static final long serialVersionUID = 6032967419751410352L;

	/**
	 * Constructor for {@link CassandraKeyspaceExistsException}.
	 *
	 * @param keyspaceName the keyspace name.
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public CassandraKeyspaceExistsException(String keyspaceName, String msg, Throwable cause) {
		super(keyspaceName, ElementType.KEYSPACE, msg, cause);
	}

	public String getKeyspaceName() {
		return getElementName();
	}
}
