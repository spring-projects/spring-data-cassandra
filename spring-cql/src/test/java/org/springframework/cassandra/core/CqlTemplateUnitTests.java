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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.cassandra.support.exception.CassandraReadTimeoutException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.querybuilder.Select;

/**
 * The CqlTemplateUnitTests class is a test suite of test cases testing the contract and functionality of the
 * {@link CqlTemplate} class.
 *
 * @author John Blum
 */
// TODO: add many more unit tests until SUT test coverage is 100%!
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class CqlTemplateUnitTests {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CqlTemplate template;

	@Mock
	private Session mockSession;

	@Before
	public void setup() {
		template = new CqlTemplate(mockSession);
		template.setExceptionTranslator(new CassandraExceptionTranslator());
	}

	@Test
	public void doExecuteInSessionCallbackIsCalled() {
		String result = template.doExecute(new SessionCallback<String>() {
			@Override public String doInSession(Session session) throws DataAccessException {
				session.execute("test");
				return "test";
			}
		});

		assertThat(result, is(equalTo("test")));

		verify(mockSession, times(1)).execute(eq("test"));
	}

	@Test
	public void doExecuteInSessionCallbackTranslatesException() {
		exception.expect(CassandraReadTimeoutException.class);
		exception.expectCause(org.hamcrest.Matchers.isA(ReadTimeoutException.class));

		template.doExecute(new SessionCallback<String>() {
			@Override public String doInSession(Session session) throws DataAccessException {
				throw new ReadTimeoutException(ConsistencyLevel.ALL, 0, 1, true);
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
}
