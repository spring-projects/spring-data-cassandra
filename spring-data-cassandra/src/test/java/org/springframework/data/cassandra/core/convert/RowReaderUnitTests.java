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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Unit tests for {@link RowReader}.
 *
 * @author Christopher Batey
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class RowReaderUnitTests {

	private static final String NON_EXISTENT_COLUMN = "column_name";

	@Mock Row row;

	@Mock ColumnDefinitions columnDefinitions;

	private RowReader underTest;

	@BeforeEach
	void setup() {

		when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
		underTest = new RowReader(row);
	}

	@Test
	void throwsIllegalArgumentExceptionIfColumnDoesNotExistByName() {

		when(columnDefinitions.firstIndexOf(NON_EXISTENT_COLUMN)).thenReturn(-1);

		assertThatIllegalArgumentException().isThrownBy(() -> underTest.get(NON_EXISTENT_COLUMN))
				.withMessageContaining("Column [%s] does not exist in table", NON_EXISTENT_COLUMN);

	}

	@Test
	void throwsIllegalArgumentExceptionIfColumnDoesNotExistByCqlIdentifier() {

		when(columnDefinitions.firstIndexOf(NON_EXISTENT_COLUMN)).thenReturn(-1);

		assertThatIllegalArgumentException().isThrownBy(() -> underTest.get(CqlIdentifier.fromCql(NON_EXISTENT_COLUMN)))
				.withMessageContaining("Column [%s] does not exist in table", NON_EXISTENT_COLUMN);
	}

	@Test
	void throwsIllegalArgumentExceptionIfColumnDoesNotExistByCqlIdentifierAndType() {

		when(columnDefinitions.firstIndexOf(NON_EXISTENT_COLUMN)).thenReturn(-1);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> underTest.get(underTest.get(CqlIdentifier.fromCql(NON_EXISTENT_COLUMN), String.class)))
				.withMessageContaining("Column [%s] does not exist in table", NON_EXISTENT_COLUMN);
	}
}
