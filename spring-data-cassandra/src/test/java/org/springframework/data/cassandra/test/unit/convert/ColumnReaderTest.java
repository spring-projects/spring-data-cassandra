package org.springframework.data.cassandra.test.unit.convert;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.ColumnReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;

@RunWith(MockitoJUnitRunner.class)
public class ColumnReaderTest {

	public static final String NON_EXISTENT_COLUMN = "column_name";

	@Mock
	private Row row;

	@Mock
	private ColumnDefinitions columnDefinitions;

	private ColumnReader underTest;

	@Before
	public void setup() {
		given(row.getColumnDefinitions()).willReturn(columnDefinitions);
		underTest = new ColumnReader(row);
	}

	@Test
	public void throwsIllegalArgumentExceptionIfColumnDoesNotExistByName() throws Exception {
		given(columnDefinitions.contains(NON_EXISTENT_COLUMN)).willReturn(false);
		given(columnDefinitions.getIndexOf(NON_EXISTENT_COLUMN)).willReturn(-1);

		try {
			underTest.get(NON_EXISTENT_COLUMN);
			fail("Expected illegal argument exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Column does not exist in Cassandra table: " + NON_EXISTENT_COLUMN, e.getMessage());
		}
	}

	@Test
	public void throwsIllegalArgumentExceptionIfColumnDoesNotExistByCqlIdentifier() throws Exception {
		given(columnDefinitions.contains(NON_EXISTENT_COLUMN)).willReturn(false);
		given(columnDefinitions.getIndexOf(NON_EXISTENT_COLUMN)).willReturn(-1);

		try {
			underTest.get(new CqlIdentifier(NON_EXISTENT_COLUMN));
			fail("Expected illegal argument exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Column does not exist in Cassandra table: " + NON_EXISTENT_COLUMN, e.getMessage());
		}
	}

	@Test
	public void throwsIllegalArgumentExceptionIfColumnDoesNotExistByCqlIdentifierAndType() throws Exception {
		given(columnDefinitions.contains(NON_EXISTENT_COLUMN)).willReturn(false);
		given(columnDefinitions.getIndexOf(NON_EXISTENT_COLUMN)).willReturn(-1);

		try {
			underTest.get(new CqlIdentifier(NON_EXISTENT_COLUMN), String.class);
			fail("Expected illegal argument exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Column does not exist in Cassandra table: " + NON_EXISTENT_COLUMN, e.getMessage());
		}
	}
}
