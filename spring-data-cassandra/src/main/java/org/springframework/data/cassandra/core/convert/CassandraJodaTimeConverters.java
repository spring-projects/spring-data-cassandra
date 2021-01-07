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

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.ClassUtils;

/**
 * Helper class to register JSR-310 specific {@link Converter} implementations to convert between Cassandra types in
 * case the library is present on the classpath.
 *
 * @author Mark Paluch
 * @since 1.5
 * @deprecated since 3.0, use JSR-310 types as replacement for Joda-Time.
 */
@Deprecated
public abstract class CassandraJodaTimeConverters {

	private static final boolean JODA_TIME_IS_PRESENT = ClassUtils.isPresent("org.joda.time.LocalDate", null);

	private CassandraJodaTimeConverters() {}

	/**
	 * Returns the converters to be registered. Will only return converters in case JodaTime is present on the class path.
	 *
	 * @return a {@link Collection} of Joda Time {@link Converter Converters} to register.
	 * @see org.springframework.core.convert.converter.Converter
	 * @see java.util.Collection
	 * @see org.joda.time
	 */
	public static Collection<Converter<?, ?>> getConvertersToRegister() {

		if (!JODA_TIME_IS_PRESENT) {
			return Collections.emptySet();
		}

		List<Converter<?, ?>> converters = new ArrayList<>();

		converters.add(MillisOfDayToLocalTimeConverter.INSTANCE);

		converters.add(FromJodaLocalTimeConverter.INSTANCE);
		converters.add(ToJodaLocalTimeConverter.INSTANCE);

		converters.add(FromJodaLocalDateConverter.INSTANCE);
		converters.add(ToJodaLocalDateConverter.INSTANCE);

		converters.add(LocalDateTimeToInstantConverter.INSTANCE);
		converters.add(InstantToLocalDateTimeConverter.INSTANCE);

		converters.add(DateTimeToInstantConverter.INSTANCE);
		converters.add(InstantToDateTimeConverter.INSTANCE);

		return converters;
	}

	/**
	 * Simple singleton to convert {@link Long}s to their {@link LocalTime} representation.
	 *
	 * @author Mark Paluch
	 */
	@Deprecated
	public enum MillisOfDayToLocalTimeConverter implements Converter<Long, LocalTime> {

		INSTANCE;

		@Override
		public LocalTime convert(Long source) {
			return LocalTime.fromMillisOfDay(source);
		}
	}

	/**
	 * Simple singleton to convert {@link LocalTime}s to their {@link Long} representation.
	 *
	 * @author Mark Paluch
	 */
	@Deprecated
	public enum LocalTimeToMillisOfDayConverter implements Converter<LocalTime, Long> {

		INSTANCE;

		@Override
		public Long convert(LocalTime source) {
			return (long) source.getMillisOfDay();
		}
	}

	/**
	 * Simple singleton to convert {@link LocalTime}s to their {@link java.time.LocalTime} representation.
	 *
	 * @author Mark Paluch
	 */
	@WritingConverter
	@Deprecated
	public enum FromJodaLocalTimeConverter implements Converter<LocalTime, java.time.LocalTime> {

		INSTANCE;

		@Override
		public java.time.LocalTime convert(LocalTime source) {
			return java.time.LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(source.getMillisOfDay()));
		}
	}

	/**
	 * Simple singleton to convert {@link java.time.LocalTime}s to their {@link LocalTime} representation.
	 *
	 * @author Mark Paluch
	 */
	@Deprecated
	public enum ToJodaLocalTimeConverter implements Converter<java.time.LocalTime, LocalTime> {

		INSTANCE;

		@Override
		public LocalTime convert(java.time.LocalTime source) {
			return LocalTime.fromMillisOfDay(TimeUnit.NANOSECONDS.toMillis(source.toNanoOfDay()));
		}
	}

	/**
	 * Simple singleton to convert {@link LocalTime}s to their {@link java.time.LocalDate} representation.
	 *
	 * @author Mark Paluch
	 */
	@WritingConverter
	@Deprecated
	public enum FromJodaLocalDateConverter implements Converter<LocalDate, java.time.LocalDate> {

		INSTANCE;

		@Override
		public java.time.LocalDate convert(LocalDate date) {
			return java.time.LocalDate.of(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
		}
	}

	/**
	 * Simple singleton to convert {@link java.time.LocalTime}s to their {@link LocalDate} representation.
	 *
	 * @author Mark Paluch
	 */
	@Deprecated
	public enum ToJodaLocalDateConverter implements Converter<java.time.LocalDate, LocalDate> {

		INSTANCE;

		@Override
		public LocalDate convert(java.time.LocalDate date) {
			return new LocalDate(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
		}
	}

	/**
	 * Simple singleton to convert {@link LocalDateTime}s to their {@link java.time.Instant} representation.
	 *
	 * @since 3.0
	 */
	@Deprecated
	public enum LocalDateTimeToInstantConverter implements Converter<LocalDateTime, java.time.Instant> {

		INSTANCE;

		@Override
		public java.time.Instant convert(LocalDateTime source) {
			return source.toDate().toInstant();
		}
	}

	/**
	 * Simple singleton to convert {@link java.time.Instant}s to their {@link LocalDateTime} representation.
	 *
	 * @since 3.0
	 */
	@Deprecated
	public enum InstantToLocalDateTimeConverter implements Converter<java.time.Instant, LocalDateTime> {

		INSTANCE;

		@Override
		public LocalDateTime convert(java.time.Instant source) {
			return new LocalDateTime(Date.from(source));
		}
	}

	/**
	 * Simple singleton to convert {@link DateTime}s to their {@link java.time.Instant} representation.
	 *
	 * @since 3.0
	 */
	@Deprecated
	public enum DateTimeToInstantConverter implements Converter<DateTime, java.time.Instant> {

		INSTANCE;

		@Override
		public java.time.Instant convert(DateTime source) {
			return source.toDate().toInstant();
		}
	}

	/**
	 * Simple singleton to convert {@link java.time.Instant}s to their {@link DateTime} representation.
	 *
	 * @since 3.0
	 */
	@Deprecated
	public enum InstantToDateTimeConverter implements Converter<java.time.Instant, DateTime> {

		INSTANCE;

		@Override
		public DateTime convert(java.time.Instant source) {
			return new DateTime(Date.from(source));
		}
	}
}
