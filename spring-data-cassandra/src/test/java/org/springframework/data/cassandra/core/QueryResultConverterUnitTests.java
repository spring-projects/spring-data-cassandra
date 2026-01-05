/*
 * Copyright 2025-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Unit tests for {@link QueryResultConverter}.
 *
 * @author Mark Paluch
 */
class QueryResultConverterUnitTests {

	static final QueryResultConverter.ConversionResultSupplier<Row> ERROR_SUPPLIER = () -> {
		throw new IllegalStateException("must not read conversion result");
	};

	@Test // GH-1568
	void converterDoesNotEagerlyRetrieveConversionResultFromSupplier() {

		QueryResultConverter<Row, String> converter = (row, reader) -> "done";

		assertThat(converter.mapRow(mock(Row.class), ERROR_SUPPLIER)).isEqualTo("done");
	}

	@Test // GH-1568
	void converterPassesOnConversionResultToNextStage() {

		Row source = mock(Row.class);

		QueryResultConverter<Row, Integer> stagedConverter = ((QueryResultConverter<Row, Integer>) (row, reader) -> 1)
				.andThen((row, reader) -> {

					assertThat(row).isEqualTo(source);
					return Integer.valueOf(reader.get());
				});

		assertThat(stagedConverter.mapRow(source, ERROR_SUPPLIER)).isEqualTo(1);
	}

	@Test // GH-1568
	void entityConverterDelaysConversion() {

		Row source = mock(Row.class);

		QueryResultConverter<Row, Integer> converter = QueryResultConverter.<Row> entity().andThen((row, reader) -> {

			assertThat(row).isEqualTo(source);
			return Integer.valueOf(20);
		});

		assertThat(converter.mapRow(source, ERROR_SUPPLIER)).isEqualTo(20);
	}

}
