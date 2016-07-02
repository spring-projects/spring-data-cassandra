/*
 *  Copyright 2013-2016 the original author or authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cassandra.core.support;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * An empty {@link com.datastax.driver.core.ResultSet} implementation
 *
 * @author John Blum
 * @see org.springframework.cassandra.core.support.ResultSetAdapter
 * @see com.datastax.driver.core.ResultSet
 * @since 1.5.0
 */
public class EmptyResultSet extends ResultSetAdapter {

	protected static final EmptyResultSet INSTANCE = new EmptyResultSet();

	/**
	 * Returns the given {@link ResultSet} if not null, otherwise returns an empty {@link ResultSet}.
	 *
	 * @param resultSet {@link ResultSet} to evaluate for {@literal null}.
	 * @return the given {@link ResultSet} if not null, otherwise return an empty {@link ResultSet}.
	 * @see com.datastax.driver.core.ResultSet
	 */
	public static ResultSet nullSafeResultSet(ResultSet resultSet) {
		return (resultSet != null ? resultSet : EmptyResultSet.INSTANCE);
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#isExhausted()
	 */
	@Override
	public boolean isExhausted() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#isFullyFetched()
	 */
	@Override
	public boolean isFullyFetched() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#getAvailableWithoutFetching()
	 */
	@Override
	public int getAvailableWithoutFetching() {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#getAllExecutionInfo()
	 */
	@Override
	public List<ExecutionInfo> getAllExecutionInfo() {
		return Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#getExecutionInfo()
	 */
	@Override
	public ExecutionInfo getExecutionInfo() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#all()
	 */
	@Override
	public List<Row> all() {
		return Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#iterator()
	 */
	@Override
	public Iterator<Row> iterator() {
		return Collections.emptyIterator();
	}

	/*
	 * (non-Javadoc)
	 * @see com.datastax.driver.core.ResultSet#one()
	 */
	@Override
	public Row one() {
		return null;
	}
}
