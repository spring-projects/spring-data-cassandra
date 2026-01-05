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
import org.springframework.lang.Nullable;

/**
 * Spring data access exception for a Cassandra write timeout.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraWriteTimeoutException extends QueryTimeoutException {

	@Serial private static final long serialVersionUID = -4374826375213670718L;

	private @Nullable String writeType;

	/**
	 * Constructor for {@link CassandraWriteTimeoutException}.
	 *
	 * @param writeType the write type.
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 * @see com.datastax.oss.driver.api.core.servererrors.WriteType
	 */
	public CassandraWriteTimeoutException(@Nullable String writeType, String msg, Throwable cause) {
		super(msg, cause);
		this.writeType = writeType;
	}

	@Nullable
	public String getWriteType() {
		return writeType;
	}
}
