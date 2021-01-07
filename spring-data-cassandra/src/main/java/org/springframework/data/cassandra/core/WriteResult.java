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
package org.springframework.data.cassandra.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * The result of a write operation.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see ResultSet
 */
public class WriteResult {

	private final boolean wasApplied;

	private final List<ExecutionInfo> executionInfo;

	private final List<Row> rows;

	WriteResult(List<ExecutionInfo> executionInfo, boolean wasApplied, List<Row> rows) {

		this.executionInfo = executionInfo;
		this.wasApplied = wasApplied;
		this.rows = rows;
	}

	WriteResult(ResultSet resultSet) {

		this.executionInfo = resultSet.getExecutionInfos();
		this.wasApplied = resultSet.wasApplied();

		int limit = resultSet.getAvailableWithoutFetching();

		List<Row> rows = new ArrayList<>(limit);

		for (int count = 0; count < limit; count++) {
			rows.add(resultSet.one());
		}

		this.rows = Collections.unmodifiableList(rows);
	}

	/**
	 * Create a {@link WriteResult} from {@link ResultSet}.
	 *
	 * @param resultSet must not be {@literal null}.
	 * @return the {@link WriteResult} for {@link ResultSet}.
	 */
	public static WriteResult of(ResultSet resultSet) {

		Assert.notNull(resultSet, "ResultSet must not be null");

		return new WriteResult(resultSet);
	}

	/**
	 * @return {@literal true} if the write was applied.
	 */
	public boolean wasApplied() {
		return wasApplied;
	}

	/**
	 * @return the list of {@link ExecutionInfo}.
	 */
	public List<ExecutionInfo> getExecutionInfo() {
		return executionInfo;
	}

	/**
	 * @return the {@link Row rows} returned by the write operation.
	 */
	public List<Row> getRows() {
		return rows;
	}
}
