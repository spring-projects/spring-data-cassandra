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

import static java.time.ZoneId.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

/**
 * Helper class to register JodaTime specific {@link Converter} implementations in case the library is present on the
 * classpath.
 *
 * @author Mark Paluch
 * @author Hurelhuyag
 * @since 1.5
 */
public abstract class CassandraJsr310Converters {

	private CassandraJsr310Converters() {}

	/**
	 * Returns the converters to be registered. Will only return converters in case we're running on Java 8.
	 *
	 * @return
	 */
	public static Collection<Converter<?, ?>> getConvertersToRegister() {

		List<Converter<?, ?>> converters = new ArrayList<>();

		converters.add(MillisOfDayToLocalTimeConverter.INSTANCE);
		converters.add(LocalTimeToMillisOfDayConverter.INSTANCE);

		converters.add(DateToInstantConverter.INSTANCE);
		converters.add(LocalDateToInstantConverter.INSTANCE);

		return converters;
	}

	/**
	 * Simple singleton to convert {@link Long}s to their {@link LocalTime} representation.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	@ReadingConverter
	public enum MillisOfDayToLocalTimeConverter implements Converter<Long, LocalTime> {

		INSTANCE;

		@Override
		public LocalTime convert(Long source) {
			return LocalTime.ofNanoOfDay(source);
		}
	}

	/**
	 * Simple singleton to convert {@link LocalTime}s to their {@link Long} representation.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 */
	@ReadingConverter
	public enum LocalTimeToMillisOfDayConverter implements Converter<LocalTime, Long> {

		INSTANCE;

		@Override
		public Long convert(LocalTime source) {
			return source.getLong(ChronoField.NANO_OF_DAY);
		}
	}

	/**
	 * Simple singleton to convert {@link Date}s to their Cassandra {@link Instant} representation for the CQL Timestamp
	 * type. Used for Cassandra 3.x to 4.x driver migration where
	 *
	 * @since 3.0
	 */
	@WritingConverter
	public enum DateToInstantConverter implements Converter<Date, Instant> {

		INSTANCE;

		@Override
		public Instant convert(Date source) {
			return source.toInstant();
		}
	}

	/**
	 * Converter from {@link LocalDateTime} to {@link Instant}.
	 *
	 * @since 3.0
	 */
	@WritingConverter
	enum LocalDateToInstantConverter implements Converter<LocalDateTime, Instant> {

		INSTANCE;

		@Override
		public Instant convert(LocalDateTime source) {
			return source.atZone(systemDefault()).toInstant();
		}
	}
}
