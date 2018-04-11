/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.threeten.bp.LocalTime;
import org.springframework.data.cassandra.core.convert.CassandraThreeTenBackPortConverters.LocalTimeToMillisOfDayConverter;
import org.springframework.data.cassandra.core.convert.CassandraThreeTenBackPortConverters.MillisOfDayToLocalTimeConverter;

/**
 * Unit tests for {@link CassandraThreeTenBackPortConverters}.
 *
 * @author Mark Paluch
 */
public class CassandraThreeTenBackPortConvertersUnitTests {

	@Test // DATACASS-302
	public void shouldConvertLongToLocalTime() {

		assertThat(MillisOfDayToLocalTimeConverter.INSTANCE.convert(3723000L))
				.isEqualTo(LocalTime.of(1, 2, 3));
	}

	@Test // DATACASS-302
	public void shouldConvertLocalTimeToLong() {

		assertThat(LocalTimeToMillisOfDayConverter.INSTANCE.convert(LocalTime.MIDNIGHT)).isZero();
		assertThat(LocalTimeToMillisOfDayConverter.INSTANCE.convert(LocalTime.of(1, 2, 3)))
				.isEqualTo(3723000L);
	}
}
