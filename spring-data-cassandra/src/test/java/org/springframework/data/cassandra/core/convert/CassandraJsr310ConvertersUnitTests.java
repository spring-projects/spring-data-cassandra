/*
 * Copyright 2018-2021 the original author or authors.
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

import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.convert.CassandraJsr310Converters.LocalTimeToMillisOfDayConverter;
import org.springframework.data.cassandra.core.convert.CassandraJsr310Converters.MillisOfDayToLocalTimeConverter;

/**
 * Unit tests for {@link CassandraJsr310Converters}.
 *
 * @author Mark Paluch
 * @author Hurelhuyag
 */
class CassandraJsr310ConvertersUnitTests {

	@Test // DATACASS-302, DATACASS-694
	void shouldConvertLongToLocalTime() {

		assertThat(MillisOfDayToLocalTimeConverter.INSTANCE.convert(3_723_000_000_000L)).isEqualTo(LocalTime.of(1, 2, 3));
	}

	@Test // DATACASS-302, DATACASS-694
	void shouldConvertLocalTimeToLong() {

		assertThat(LocalTimeToMillisOfDayConverter.INSTANCE.convert(LocalTime.MIDNIGHT)).isZero();
		assertThat(LocalTimeToMillisOfDayConverter.INSTANCE.convert(LocalTime.of(1, 2, 3))).isEqualTo(3_723_000_000_000L);
	}
}
