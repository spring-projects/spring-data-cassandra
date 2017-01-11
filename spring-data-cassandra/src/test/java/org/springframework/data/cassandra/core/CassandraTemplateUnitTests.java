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

package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.test.integration.simpletons.Book;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Test suite of test cases testing the contract and functionality of the {@link CassandraTemplate} class.
 *
 * @author John Blum
 * @see org.springframework.data.cassandra.core.CassandraTemplate
 * @since 1.5.0
 */
@RunWith(MockitoJUnitRunner.class)
public class CassandraTemplateUnitTests {

	private CassandraTemplate template;

	@Mock private Session mockSession;

	@Before
	public void setup() {
		template = new CassandraTemplate(mockSession);
	}

	protected <T> Iterator<T> iterator(T... elements) {
		return Collections.unmodifiableList(Arrays.asList(elements)).iterator();
	}

	protected Row mockRow(String name) {
		return mock(Row.class, name);
	}

	protected <T> CassandraConverterRowCallback<T> newRollCallback(CassandraConverter converter, Class<T> type) {
		return new CassandraConverterRowCallback<T>(converter, type);
	}

	@Test // DATACASS-310
	public void processResultSetHandlesResultSetRows() {
		ResultSet mockResultSet = mock(ResultSet.class);

		Row mockRowOne = mockRow("MockRowOne");
		Row mockRowTwo = mockRow("MockRowTwo");
		Row mockRowThree = mockRow("MockRowThree");

		CassandraConverter mockCassandraConverter = mock(CassandraConverter.class);

		when(mockSession.execute(eq("SELECT * FROM Test"))).thenReturn(mockResultSet);
		when(mockResultSet.iterator()).thenReturn(iterator(mockRowOne, mockRowTwo, mockRowThree));
		when(mockCassandraConverter.read(eq(Integer.class), eq(mockRowOne))).thenReturn(1);
		when(mockCassandraConverter.read(eq(Integer.class), eq(mockRowTwo))).thenReturn(2);
		when(mockCassandraConverter.read(eq(Integer.class), eq(mockRowThree))).thenReturn(3);

		List<Integer> results = template.select("SELECT * FROM Test",
				newRollCallback(mockCassandraConverter, Integer.class));

		assertThat(results).isNotNull().hasSize(3).contains(1, 2, 3);

		verify(mockSession, times(1)).execute(eq("SELECT * FROM Test"));
		verify(mockResultSet, times(1)).iterator();
		verify(mockCassandraConverter, times(1)).read(eq(Integer.class), eq(mockRowOne));
		verify(mockCassandraConverter, times(1)).read(eq(Integer.class), eq(mockRowTwo));
		verify(mockCassandraConverter, times(1)).read(eq(Integer.class), eq(mockRowThree));
	}

	@Test // DATACASS-310
	public void processResultSetHandlesSingleElementResultSet() {
		Select mockSelect = mock(Select.class);
		ResultSet mockResultSet = mock(ResultSet.class);
		Row mockRow = mock(Row.class);
		CassandraConverter mockCassandraConverter = mock(CassandraConverter.class);

		when(mockSession.execute(eq(mockSelect))).thenReturn(mockResultSet);
		when(mockResultSet.iterator()).thenReturn(iterator(mockRow));
		when(mockCassandraConverter.read(eq(String.class), eq(mockRow))).thenReturn("test");

		List<String> results = template.select(mockSelect, newRollCallback(mockCassandraConverter, String.class));

		assertThat(results).hasSize(1).contains("test");

		verify(mockSession, times(1)).execute(eq(mockSelect));
		verify(mockResultSet, times(1)).iterator();
		verify(mockCassandraConverter, times(1)).read(eq(String.class), eq(mockRow));
	}

	@Test // DATACASS-310
	public void processResultSetHandlesEmptyResultSet() {
		CassandraConverter mockCassandraConverter = mock(CassandraConverter.class);
		ResultSet mockResultSet = mock(ResultSet.class);

		when(mockSession.execute(eq("SELECT * FROM Test"))).thenReturn(mockResultSet);
		when(mockResultSet.iterator()).thenReturn(this.<Row> iterator());

		List<Object> results = template.select("SELECT * FROM Test", newRollCallback(mockCassandraConverter, Object.class));

		assertThat(results).isNotNull();
		assertThat(results.isEmpty()).isTrue();

		verify(mockSession, times(1)).execute(eq("SELECT * FROM Test"));
		verify(mockResultSet, times(1)).iterator();
		verifyZeroInteractions(mockCassandraConverter);
	}

	@Test // DATACASS-310
	public void processResultSetHandlesNullResultSet() {
		CassandraConverter mockCassandraConverter = mock(CassandraConverter.class);

		when(mockSession.execute(anyString())).thenReturn(null);

		List<Object> results = template.select("SELECT * FROM Test", newRollCallback(mockCassandraConverter, Object.class));

		assertThat(results).isNotNull();
		assertThat(results.isEmpty()).isTrue();

		verify(mockSession, times(1)).execute(eq("SELECT * FROM Test"));
		verifyZeroInteractions(mockCassandraConverter);
	}

	@Test // DATACASS-288
	public void batchOperationsShouldCallSession() {
		template.batchOps().insert(new Book()).execute();

		verify(mockSession).execute(Mockito.any(Batch.class));
	}
}
