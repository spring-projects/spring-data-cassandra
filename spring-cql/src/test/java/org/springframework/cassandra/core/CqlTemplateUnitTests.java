/*
 *  Copyright 2016 the original author or authors
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
package org.springframework.cassandra.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.cassandra.support.exception.CassandraReadTimeoutException;
import org.springframework.cassandra.support.exception.CassandraUncategorizedException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Using;

/**
 * The CqlTemplateUnitTests class is a test suite of test cases testing the contract and functionality of the
 * {@link CqlTemplate} class.
 *
 * @author John Blum
 * @author Mark Paluch
 */
// TODO: add many more unit tests until SUT test coverage is 100%!
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class CqlTemplateUnitTests {

	@Rule public ExpectedException exception = ExpectedException.none();

	private CqlTemplate template;

	@Mock private Insert mockInsert;

	@Mock private PreparedStatement mockPreparedStatement;

	@Mock private Session mockSession;

	@Mock private Statement mockStatement;

	@Mock private Update mockUpdate;

	@Before
	public void setup() {
		template = new CqlTemplate(mockSession);
		template.setExceptionTranslator(new CassandraExceptionTranslator());
	}

	@Test
	public void doExecuteInSessionCallbackIsCalled() {
		String result = template.doExecute(new SessionCallback<String>() {
			@Override
			public String doInSession(Session session) throws DataAccessException {
				session.execute("test");
				return "test";
			}
		});

		assertThat(result, is(equalTo("test")));

		verify(mockSession, times(1)).execute(eq("test"));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-304">DATACASS-304</a>
	 */
	@Test
	public void doExecuteInSessionCallbackTranslatesToCassandraException() {
		exception.expect(CassandraReadTimeoutException.class);
		exception.expectCause(org.hamcrest.Matchers.isA(ReadTimeoutException.class));

		template.doExecute(new SessionCallback<String>() {
			@Override
			public String doInSession(Session session) throws DataAccessException {
				throw new ReadTimeoutException(ConsistencyLevel.ALL, 0, 1, true);
			}
		});
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-304">DATACASS-304</a>
	 */
	@Test
	public void doExecuteInSessionCallbackTranslatesToCassandraUncategorizedException() {
		exception.expect(CassandraUncategorizedException.class);
		exception.expectCause(org.hamcrest.Matchers.isA(DriverException.class));
		exception.expectMessage(containsString("test"));

		template.doExecute(new SessionCallback<String>() {
			@Override
			public String doInSession(Session session) throws DataAccessException {
				throw new DriverException("test");
			}
		});
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-304">DATACASS-304</a>
	 */
	@Test
	public void doExecuteInSessionCallbackTranslatesToCassandraUncategorizedDataAccessException() {
		exception.expect(CassandraUncategorizedDataAccessException.class);
		exception.expectCause(org.hamcrest.Matchers.isA(Error.class));
		exception.expectMessage(containsString("test"));

		template.doExecute(new SessionCallback<String>() {
			@Override
			public String doInSession(Session session) throws DataAccessException {
				throw new Error("test");
			}
		});
	}

	@Test
	public void doExecuteWithNullSessionCallbackThrowsIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));
		exception.expectMessage("SessionCallback must not be null");

		template.doExecute((SessionCallback) null);
	}

	@Test
	public void doExecuteQueryReturnsResultSetForOqlQueryString() {
		ResultSet mockResultSet = mock(ResultSet.class);

		when(mockSession.execute(eq("SELECT * FROM Customers"))).thenReturn(mockResultSet);

		ResultSet resultSet = template.doExecuteQueryReturnResultSet("SELECT * FROM Customers");

		assertThat(resultSet, is(equalTo(mockResultSet)));

		verify(mockSession, times(1)).execute(eq("SELECT * FROM Customers"));
		verifyZeroInteractions(mockResultSet);
	}

	@Test
	public void doExecuteSelectReturnsResultSetForOqlQueryString() {
		Select mockSelect = mock(Select.class);
		ResultSet mockResultSet = mock(ResultSet.class);

		when(mockSession.execute(eq(mockSelect))).thenReturn(mockResultSet);

		ResultSet resultSet = template.doExecuteQueryReturnResultSet(mockSelect);

		assertThat(resultSet, is(equalTo(mockResultSet)));

		verify(mockSession, times(1)).execute(eq(mockSelect));
		verifyZeroInteractions(mockResultSet);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void firstColumnToObjectReturnsColumnValue() {

		final Row mockRow = mock(Row.class);
		ColumnDefinitions mockColumnDefinitions = mock(ColumnDefinitions.class);
		Iterator<ColumnDefinitions.Definition> mockIterator = mock(Iterator.class);
		final ColumnDefinitions.Definition mockColumnDefinition = mock(ColumnDefinitions.Definition.class);

		when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
		when(mockColumnDefinitions.iterator()).thenReturn(mockIterator);
		when(mockIterator.hasNext()).thenReturn(true);
		when(mockIterator.next()).thenReturn(mockColumnDefinition);

		template = new CqlTemplate() {
			@Override
			<T> T columnToObject(Row row, ColumnDefinitions.Definition columnDefinition) {
				assertThat(row, is(sameInstance(mockRow)));
				assertThat(columnDefinition, is(sameInstance(mockColumnDefinition)));

				return (T) "test";
			}
		};

		assertThat(String.valueOf(template.firstColumnToObject(mockRow)), is(equalTo("test")));

		verify(mockRow, times(1)).getColumnDefinitions();
		verify(mockColumnDefinitions, times(1)).iterator();
		verify(mockIterator, times(1)).hasNext();
		verify(mockIterator, times(1)).next();
		verifyZeroInteractions(mockColumnDefinition);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void firstColumnToObjectReturnsNull() {

		Row mockRow = mock(Row.class);
		ColumnDefinitions mockColumnDefinitions = mock(ColumnDefinitions.class);
		Iterator<ColumnDefinitions.Definition> mockIterator = mock(Iterator.class);

		when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
		when(mockColumnDefinitions.iterator()).thenReturn(mockIterator);
		when(mockIterator.hasNext()).thenReturn(false);

		assertThat(template.firstColumnToObject(mockRow), is(nullValue(Object.class)));

		verify(mockRow, times(1)).getColumnDefinitions();
		verify(mockColumnDefinitions, times(1)).iterator();
		verify(mockIterator, times(1)).hasNext();
		verify(mockIterator, never()).next();
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void processOneIsSuccessful() {

		ResultSet mockResultSet = mock(ResultSet.class);
		Row mockRow = mock(Row.class);
		RowMapper<String> mockRowMapper = mock(RowMapper.class);

		when(mockResultSet.one()).thenReturn(mockRow);
		when(mockResultSet.isExhausted()).thenReturn(true);
		when(mockRowMapper.mapRow(eq(mockRow), eq(0))).thenReturn("test");

		assertThat(template.processOne(mockResultSet, mockRowMapper), is(equalTo("test")));

		verify(mockResultSet, times(1)).one();
		verify(mockResultSet, times(1)).isExhausted();
		verify(mockRowMapper, times(1)).mapRow(eq(mockRow), eq(0));
		verifyZeroInteractions(mockRow);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void processOneThrowsIncorrectResultSetSizeDataAccessExceptionWhenNoRowsFound() {

		ResultSet mockResultSet = mock(ResultSet.class);
		RowMapper mockRowMapper = mock(RowMapper.class);

		when(mockResultSet.one()).thenReturn(null);

		try {
			exception.expect(IncorrectResultSizeDataAccessException.class);
			exception.expectCause(is(nullValue(Throwable.class)));
			exception.expectMessage(containsString("expected 1, actual 0"));

			template.processOne(mockResultSet, mockRowMapper);

		} finally {
			verify(mockResultSet, times(1)).one();
			verify(mockResultSet, never()).isExhausted();
			verifyZeroInteractions(mockRowMapper);
		}
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void processOneThrowsIncorrectResultSetSizeDataAccessExceptionWhenTooManyRowsFound() {

		ResultSet mockResultSet = mock(ResultSet.class);
		Row mockRow = mock(Row.class);
		RowMapper mockRowMapper = mock(RowMapper.class);

		when(mockResultSet.one()).thenReturn(mockRow);
		when(mockResultSet.isExhausted()).thenReturn(false);

		try {
			exception.expect(IncorrectResultSizeDataAccessException.class);
			exception.expectCause(is(nullValue(Throwable.class)));
			exception.expectMessage("ResultSet size exceeds 1");

			template.processOne(mockResultSet, mockRowMapper);

		} finally {
			verify(mockResultSet, times(1)).one();
			verify(mockResultSet, times(1)).isExhausted();
			verifyZeroInteractions(mockRowMapper);
			verifyZeroInteractions(mockRow);
		}
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void processOnePassingNullResultSetThrowsIllegalArgumentException() {

		RowMapper mockRowMapper = mock(RowMapper.class);

		try {
			exception.expect(IllegalArgumentException.class);
			exception.expectCause(is(nullValue(Throwable.class)));

			template.processOne(null, mockRowMapper);
		} finally {
			verifyZeroInteractions(mockRowMapper);
		}
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void processOneWithRequiredTypeIsSuccessful() {

		ResultSet mockResultSet = mock(ResultSet.class);
		final Row mockRow = mock(Row.class);

		when(mockResultSet.one()).thenReturn(mockRow);
		when(mockResultSet.isExhausted()).thenReturn(true);

		template = new CqlTemplate() {
			@Override
			protected Object firstColumnToObject(Row row) {
				assertThat(row, is(equalTo(mockRow)));
				return 1L;
			}
		};

		Number value = template.processOne(mockResultSet, Long.class);

		assertThat(value, is(instanceOf(Long.class)));
		assertThat(value.longValue(), is(equalTo(1L)));

		verify(mockResultSet, times(1)).one();
		verify(mockResultSet, times(1)).isExhausted();
		verifyZeroInteractions(mockRow);
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void processOneWithRequiredTypeThrowsIncorrectResultSetSizeDataAccessExceptionWhenNoRowsFound() {

		ResultSet mockResultSet = mock(ResultSet.class);

		when(mockResultSet.one()).thenReturn(null);

		try {
			exception.expect(IncorrectResultSizeDataAccessException.class);
			exception.expectCause(is(nullValue(Throwable.class)));
			exception.expectMessage(containsString("expected 1, actual 0"));

			template.processOne(mockResultSet, Integer.class);

		} finally {
			verify(mockResultSet, times(1)).one();
			verify(mockResultSet, never()).isExhausted();
		}
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void processOneWithRequiredTypeThrowsIncorrectResultSetSizeDataAccessExceptionWhenTooManyRowsFound() {

		ResultSet mockResultSet = mock(ResultSet.class);
		Row mockRow = mock(Row.class);

		when(mockResultSet.one()).thenReturn(mockRow);
		when(mockResultSet.isExhausted()).thenReturn(false);

		try {
			exception.expect(IncorrectResultSizeDataAccessException.class);
			exception.expectCause(is(nullValue(Throwable.class)));
			exception.expectMessage(containsString("ResultSet size exceeds 1"));

			template.processOne(mockResultSet, Double.class);

		} finally {
			verify(mockResultSet, times(1)).one();
			verify(mockResultSet, times(1)).isExhausted();
			verifyZeroInteractions(mockRow);
		}
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-286">DATACASS-286</a>
	 */
	@Test
	public void processOneWithRequiredTypePassingNullResultSetThrowsIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);
		exception.expectCause(is(nullValue(Throwable.class)));

		template.processOne(null, String.class);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void addPreparedStatementOptionsShouldAddDriverQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.build();

		template.addPreparedStatementOptions(mockPreparedStatement, queryOptions);

		verify(mockPreparedStatement).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockPreparedStatement).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void addPreparedStatementOptionsShouldAddOurQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().retryPolicy(RetryPolicy.FALLTHROUGH).build();

		queryOptions.setConsistencyLevel(org.springframework.cassandra.core.ConsistencyLevel.LOCAL_QUOROM);

		template.addPreparedStatementOptions(mockPreparedStatement, queryOptions);

		verify(mockPreparedStatement).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockPreparedStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void addStatementQueryOptionsShouldAddDriverQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder()
				.consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.build();

		template.addQueryOptions(mockStatement, queryOptions);

		verify(mockStatement).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockStatement).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void addStatementQueryOptionsShouldAddOurQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder().retryPolicy(RetryPolicy.FALLTHROUGH).build();

		queryOptions.setConsistencyLevel(org.springframework.cassandra.core.ConsistencyLevel.LOCAL_QUOROM);

		template.addQueryOptions(mockStatement, queryOptions);

		verify(mockStatement).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void addStatementQueryOptionsShouldNotAddOptions() {

		QueryOptions queryOptions = QueryOptions.builder().build();

		template.addQueryOptions(mockStatement, queryOptions);

		verifyZeroInteractions(mockStatement);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void addStatementQueryOptionsShouldAddGenericQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.fetchSize(10) //
				.readTimeout(1, TimeUnit.MINUTES) //
				.withTracing() //
				.build();

		template.addQueryOptions(mockStatement, queryOptions);

		verify(mockStatement).setReadTimeoutMillis(60 * 1000);
		verify(mockStatement).setFetchSize(10);
		verify(mockStatement).enableTracing();
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void addInsertWriteOptionsShouldAddDriverQueryOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.readTimeout(10) //
				.ttl(10) //
				.build();

		template.addWriteOptions(mockInsert, writeOptions);

		verify(mockInsert).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockInsert).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockInsert).setReadTimeoutMillis(10);
		verify(mockInsert).using(Mockito.any(Using.class));
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void addUpdateWriteOptionsShouldAddDriverQueryOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(ConsistencyLevel.EACH_QUORUM) //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.ttl(10) //
				.tracing(false)
				.build();

		template.addWriteOptions(mockUpdate, writeOptions);

		verify(mockUpdate).setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
		verify(mockUpdate).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		verify(mockUpdate).using(Mockito.any(Using.class));
		verify(mockUpdate).disableTracing();
	}
}
