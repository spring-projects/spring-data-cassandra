/*
 * Copyright 2016-2021 the original author or authors.
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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import org.springframework.lang.Nullable;

/**
 * Simple adapter for {@link PreparedStatementBinder} that applies a given array of arguments.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class ArgumentPreparedStatementBinder implements PreparedStatementBinder {

	private final @Nullable Object[] args;

	/**
	 * Create a new {@link ArgumentPreparedStatementBinder} for the given arguments.
	 *
	 * @param args the arguments to set. May be empty or {@code null} if no arguments are provided.
	 */
	public ArgumentPreparedStatementBinder(@Nullable Object... args) {
		this.args = args;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.PreparedStatementBinder#bindValues(com.datastax.driver.core.PreparedStatement)
	 */
	@Override
	public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
		return args != null ? ps.bind(args) : ps.bind();
	}
}
