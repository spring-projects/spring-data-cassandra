/*
 *  Copyright 2013-2019 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cassandra.core.support;

import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * An Adapter class to simply implementations of the {@link ResultSet} interface.
 *
 * @author John Blum
 * @see com.datastax.driver.core.ResultSet
 * @since 1.5.0
 */
public class ResultSetAdapter implements ResultSet {

	private static final String NOT_SUPPORTED = "Not Supported";

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#isExhausted()
	 */
	@Override
	public boolean isExhausted() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#isFullyFetched()
	 */
	@Override
	public boolean isFullyFetched() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#getAvailableWithoutFetching()
	 */
	@Override
	public int getAvailableWithoutFetching() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#getColumnDefinitions()
	 */
	@Override
	public ColumnDefinitions getColumnDefinitions() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#getAllExecutionInfo()
	 */
	@Override
	public List<ExecutionInfo> getAllExecutionInfo() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#getExecutionInfo()
	 */
	@Override
	public ExecutionInfo getExecutionInfo() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#all()
	 */
	@Override
	public List<Row> all() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#fetchMoreResults()
	 */
	@Override
	public ListenableFuture<ResultSet> fetchMoreResults() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#iterator()
	 */
	@Override
	public Iterator<Row> iterator() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#one()
	 */
	@Override
	public Row one() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#wasApplied()
	 */
	@Override
	public boolean wasApplied() {
		throw new UnsupportedOperationException(NOT_SUPPORTED);
	}
}
