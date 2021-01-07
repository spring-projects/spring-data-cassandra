/*
 * Copyright 2013-2021 the original author or authors.
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

import org.springframework.dao.DataAccessException;

/**
 * Spring data access exception for a Cassandra internal error.
 *
 * @author Matthew T. Adams
 */
public class CassandraInternalException extends DataAccessException {

	private static final long serialVersionUID = 433061676465346338L;

	public CassandraInternalException(String msg) {
		super(msg);
	}

	public CassandraInternalException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
