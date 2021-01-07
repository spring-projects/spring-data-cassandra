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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.ThreeTenBackPortConverters;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.ClassUtils;

import org.threeten.bp.LocalDate;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.LocalTime;
import org.threeten.bp.ZoneId;
import org.threeten.bp.temporal.ChronoField;

/**
 * Helper class to register {@link Converter} implementations for the ThreeTen Backport project in case it's present on
 * the classpath.
 *
 * @author Mark Paluch
 * @see <a href="https://www.threeten.org/threetenbp">Threeten Backport</a>
 * @since 1.5
 * @deprecated since 3.0, use JSR-310 types as replacement for ThreeTen Backport.
 */
@Deprecated
public abstract class CassandraThreeTenBackPortConverters {

	private static final boolean THREE_TEN_BACK_PORT_IS_PRESENT = ClassUtils.isPresent("org.threeten.bp.LocalDateTime",
			ThreeTenBackPortConverters.class.getClassLoader());

	private CassandraThreeTenBackPortConverters() {}

	/**
	 * Returns the converters to be registered. Will only return converters in case ThreeTen Backport is on the class
	 * path.
	 *
	 * @return a {@link Collection} of ThreeTen Backport {@link Converter Converters} to register.
	 * @see org.springframework.core.convert.converter.Converter
	 * @see java.util.Collection
	 * @see org.joda.time
	 */
	public static Collection<Converter<?, ?>> getConvertersToRegister() {

		if (!THREE_TEN_BACK_PORT_IS_PRESENT) {
			return Collections.emptySet();
		}

		List<Converter<?, ?>> converters = new ArrayList<>();

		converters.add(MillisOfDayToLocalTimeConverter.INSTANCE);
		converters.add(LocalTimeToMillisOfDayConverter.INSTANCE);

		converters.add(FromBpLocalTimeConverter.INSTANCE);
		converters.add(ToBpLocalTimeConverter.INSTANCE);

		converters.add(FromBpLocalDateConverter.INSTANCE);
		converters.add(ToBpLocalDateConverter.INSTANCE);

		converters.add(FromBpLocalDateTimeConverter.INSTANCE);
		converters.add(ToBpLocalDateTimeConverter.INSTANCE);

		converters.add(LocalDateTimeToInstantConverter.INSTANCE);
		converters.add(BpInstantToInstantConverter.INSTANCE);
		converters.add(InstantToBpInstantConverter.INSTANCE);

		converters.add(ZoneIdToStringConverter.INSTANCE);
		converters.add(StringToZoneIdConverter.INSTANCE);

		return converters;
	}

	/**
	 * Simple singleton to convert {@link Long}s to their {@link LocalTime} representation.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	@Deprecated
	public enum MillisOfDayToLocalTimeConverter implements Converter<Long, LocalTime> {

		INSTANCE;

		@Override
		public LocalTime convert(Long source) {
			return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(source));
		}
	}

	/**
	 * Simple singleton to convert {@link LocalTime}s to their {@link Long} representation.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	@ReadingConverter
	@Deprecated
	public enum LocalTimeToMillisOfDayConverter implements Converter<LocalTime, Long> {

		INSTANCE;

		@Override
		public Long convert(LocalTime source) {
			return source.getLong(ChronoField.MILLI_OF_DAY);
		}
	}

	/**
	 * Simple singleton to convert {@link LocalTime}s to their {@link java.time.LocalTime} representation.
	 *
	 * @since 3.0
	 */
	@WritingConverter
	@Deprecated
	public enum FromBpLocalTimeConverter implements Converter<LocalTime, java.time.LocalTime> {

		INSTANCE;

		@Override
		public java.time.LocalTime convert(LocalTime source) {
			return java.time.LocalTime.ofNanoOfDay(source.toNanoOfDay());
		}
	}

	/**
	 * Simple singleton to convert {@link java.time.LocalTime}s to their {@link LocalTime} representation.
	 *
	 * @since 3.0
	 */
	@Deprecated
	public enum ToBpLocalTimeConverter implements Converter<java.time.LocalTime, LocalTime> {

		INSTANCE;

		@Override
		public LocalTime convert(java.time.LocalTime source) {
			return LocalTime.ofNanoOfDay(source.toNanoOfDay());
		}
	}

	/**
	 * Simple singleton to convert {@link LocalTime}s to their {@link java.time.LocalDate} representation.
	 *
	 * @since 3.0
	 */
	@WritingConverter
	@Deprecated
	public enum FromBpLocalDateConverter implements Converter<LocalDate, java.time.LocalDate> {

		INSTANCE;

		@Override
		public java.time.LocalDate convert(LocalDate date) {
			return java.time.LocalDate.of(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
		}
	}

	/**
	 * Simple singleton to convert {@link java.time.LocalTime}s to their {@link LocalDate} representation.
	 *
	 * @since 3.0
	 */
	@Deprecated
	public enum ToBpLocalDateConverter implements Converter<java.time.LocalDate, LocalDate> {

		INSTANCE;

		@Override
		public LocalDate convert(java.time.LocalDate date) {
			return LocalDate.of(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
		}
	}

	/**
	 * Simple singleton to convert {@link LocalDateTime}s to their {@link java.time.LocalDateTime} representation.
	 *
	 * @since 3.0
	 */
	@ReadingConverter
	@Deprecated
	public enum FromBpLocalDateTimeConverter implements Converter<LocalDateTime, java.time.LocalDateTime> {

		INSTANCE;

		@Override
		public java.time.LocalDateTime convert(LocalDateTime date) {
			return java.time.LocalDateTime.of(date.getYear(), date.getMonthValue(), date.getDayOfMonth(), date.getHour(),
					date.getMinute(), date.getSecond(), date.getNano());
		}
	}

	/**
	 * Simple singleton to convert {@link java.time.LocalDateTime}s to their {@link LocalDateTime} representation.
	 *
	 * @since 3.0
	 */
	@Deprecated
	public enum ToBpLocalDateTimeConverter implements Converter<java.time.LocalDateTime, LocalDateTime> {

		INSTANCE;

		@Override
		public LocalDateTime convert(java.time.LocalDateTime date) {
			return LocalDateTime.of(date.getYear(), date.getMonthValue(), date.getDayOfMonth(), date.getHour(),
					date.getMinute(), date.getSecond(), date.getNano());
		}
	}

	/**
	 * Convert {@link LocalDateTime} to {@link Instant}.
	 *
	 * @since 3.0
	 */
	@Deprecated
	enum LocalDateTimeToInstantConverter implements Converter<LocalDateTime, java.time.Instant> {

		INSTANCE;

		@Override
		public java.time.Instant convert(LocalDateTime source) {
			return Instant.ofEpochMilli(source.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
		}
	}

	/**
	 * Convert {@link org.threeten.bp.Instant} to {@link java.time.Instant}.
	 *
	 * @since 3.0
	 */
	@Deprecated
	enum BpInstantToInstantConverter implements Converter<org.threeten.bp.Instant, java.time.Instant> {

		INSTANCE;

		@Override
		public java.time.Instant convert(org.threeten.bp.Instant source) {
			return Instant.ofEpochMilli(source.toEpochMilli());
		}
	}

	/**
	 * Convert {@link java.time.Instant} to {@link org.threeten.bp.Instant}.
	 *
	 * @since 3.0
	 */
	@Deprecated
	enum InstantToBpInstantConverter implements Converter<java.time.Instant, org.threeten.bp.Instant> {

		INSTANCE;

		@Override
		public org.threeten.bp.Instant convert(java.time.Instant source) {
			return org.threeten.bp.Instant.ofEpochMilli(source.toEpochMilli());
		}
	}

	/**
	 * Convert {@link ZoneId} to {@link String}.
	 *
	 * @since 3.0
	 */
	@Deprecated
	enum ZoneIdToStringConverter implements Converter<ZoneId, String> {

		INSTANCE;

		@Override
		public String convert(ZoneId source) {
			return source.toString();
		}
	}

	/**
	 * Convert {@link String} to {@link ZoneId}.
	 *
	 * @since 3.0
	 */
	@Deprecated
	enum StringToZoneIdConverter implements Converter<String, ZoneId> {

		INSTANCE;

		@Override
		public ZoneId convert(String source) {
			return ZoneId.of(source);
		}
	}
}
