/*
 *  Copyright 2013-2017 the original author or authors
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Iterator;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * Test suite of test cases testing the contract and functionality of the {@link EmptyResultSet}.
 *
 * @author John Blum
 * @see org.springframework.cassandra.core.support.EmptyResultSet
 * @since 1.5.0
 */
public class EmptyResultSetUnitTests {

	@Test
	public void nullSafeResultSetReturnsGivenResultSet() {
		ResultSet mockResultSet = mock(ResultSet.class);
		ResultSet theResultSet = EmptyResultSet.nullSafeResultSet(mockResultSet);

		assertThat(theResultSet).isSameAs(mockResultSet);
	}

	@Test
	public void nullSAfeResultSetReturnsEmptyResultSetForNull() {
		ResultSet resultSet = EmptyResultSet.nullSafeResultSet(null);

		assertThat(resultSet).isInstanceOf(EmptyResultSet.class);
	}

	@Test
	public void isExhaustedForEmptyResultIsTrue() {
		assertThat(EmptyResultSet.nullSafeResultSet(null).isExhausted()).isTrue();
	}

	@Test
	public void isFullyFetchedForEmptyResultSetIsTrue() {
		assertThat(EmptyResultSet.nullSafeResultSet(null).isFullyFetched()).isTrue();
	}

	@Test
	public void getAllExecutionInfoForEmptyResultSetIsEmptyList() {
		assertThat(EmptyResultSet.nullSafeResultSet(null).getAllExecutionInfo()).isEmpty();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getColumnDefinitionsForEmptyResultSetThrowsUnsupportedOperationException() {
		EmptyResultSet.nullSafeResultSet(null).getColumnDefinitions();
	}

	@Test
	public void getExecutionInfoForEmptyResultSetIsNull() {
		assertThat(EmptyResultSet.nullSafeResultSet(null).getExecutionInfo()).isNull();
	}

	@Test
	public void allForEmptyResultSetIsEmptyList() {
		assertThat(EmptyResultSet.nullSafeResultSet(null).all()).isEmpty();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void fetchMoreResultsFromEmptyResultSetThrowsUnsupportedOperationException() {
		EmptyResultSet.nullSafeResultSet(null).fetchMoreResults();
	}

	@Test
	public void iteratorForEmptyResultSetIsEmptyIterator() {
		Iterator<Row> iterator = EmptyResultSet.nullSafeResultSet(null).iterator();

		assertThat(iterator).isNotNull();
		assertThat(iterator.hasNext()).isFalse();
	}

	@Test
	public void oneForEmptyResultSetIsNull() {
		assertThat(EmptyResultSet.nullSafeResultSet(null).one()).isNull();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void wasAppliedOnEmptyResultSetThrowsUnsupportedOperationException() {
		EmptyResultSet.nullSafeResultSet(null).wasApplied();
	}
}
