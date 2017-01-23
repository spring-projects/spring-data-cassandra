/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateMidnight;
import org.joda.time.LocalDate;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ClassUtils;

/**
 * Helper class to register JSR-310 specific {@link Converter} implementations to convert between Cassandra types in
 * case the library is present on the classpath.
 *
 * @author Mark Paluch
 * @since 1.5
 */
@SuppressWarnings("deprecation")
public abstract class CassandraJodaTimeConverters {

	private static final boolean JODA_TIME_IS_PRESENT = ClassUtils.isPresent("org.joda.time.LocalDate", null);

	private CassandraJodaTimeConverters() {}

	/**
	 * Returns the converters to be registered. Will only return converters in case JodaTime is present on the class path.
	 *
	 * @return
	 */
	public static Collection<Converter<?, ?>> getConvertersToRegister() {

		if (!JODA_TIME_IS_PRESENT) {
			return Collections.emptySet();
		}

		List<Converter<?, ?>> converters = new ArrayList<>();

		converters.add(CassandraLocalDateToLocalDateConverter.INSTANCE);
		converters.add(LocalDateToCassandraLocalDateConverter.INSTANCE);
		converters.add(CassandraLocalDateToDateMidnightConverter.INSTANCE);
		converters.add(DateMidnightToCassandraLocalDateConverter.INSTANCE);

		return converters;
	}

	/**
	 * Simple singleton to convert {@link com.datastax.driver.core.LocalDate}s to their {@link LocalDate} representation.
	 *
	 * @author Mark Paluch
	 */
	public enum CassandraLocalDateToLocalDateConverter
			implements Converter<com.datastax.driver.core.LocalDate, LocalDate> {

		INSTANCE;

		@Override
		public LocalDate convert(com.datastax.driver.core.LocalDate source) {
			return new LocalDate(source.getYear(), source.getMonth(), source.getDay());
		}
	}

	/**
	 * Simple singleton to convert {@link LocalDate}s to their {@link com.datastax.driver.core.LocalDate} representation.
	 *
	 * @author Mark Paluch
	 */
	public enum LocalDateToCassandraLocalDateConverter
			implements Converter<LocalDate, com.datastax.driver.core.LocalDate> {

		INSTANCE;

		@Override
		public com.datastax.driver.core.LocalDate convert(LocalDate source) {
			return com.datastax.driver.core.LocalDate.fromYearMonthDay(source.getYear(), source.getMonthOfYear(),
					source.getDayOfMonth());
		}
	}

	/**
	 * Simple singleton to convert {@link com.datastax.driver.core.LocalDate}s to their {@link DateMidnight}
	 * representation.
	 *
	 * @author Mark Paluch
	 */
	public enum CassandraLocalDateToDateMidnightConverter
			implements Converter<com.datastax.driver.core.LocalDate, DateMidnight> {

		INSTANCE;

		@Override
		public DateMidnight convert(com.datastax.driver.core.LocalDate source) {
			return new DateMidnight(source.getYear(), source.getMonth(), source.getDay());
		}
	}

	/**
	 * Simple singleton to convert {@link DateMidnight}s to their {@link com.datastax.driver.core.LocalDate}
	 * representation.
	 *
	 * @author Mark Paluch
	 */
	public enum DateMidnightToCassandraLocalDateConverter
			implements Converter<DateMidnight, com.datastax.driver.core.LocalDate> {

		INSTANCE;

		@Override
		public com.datastax.driver.core.LocalDate convert(DateMidnight source) {
			return com.datastax.driver.core.LocalDate.fromYearMonthDay(source.getYear(), source.getMonthOfYear(),
					source.getDayOfMonth());
		}
	}
}
