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

import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.data.cassandra.core.cql.converter.RowToListConverter;
import org.springframework.data.cassandra.core.cql.converter.RowToMapConverter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Wrapper class to contain useful converters for the usage with Cassandra.
 *
 * @author Mark Paluch
 * @since 1.5
 */
abstract class CassandraConverters {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private CassandraConverters() {}

	/**
	 * Returns the converters to be registered.
	 */
	static Collection<Object> getConvertersToRegister() {

		List<Object> converters = new ArrayList<>();

		converters.add(RowToCassandraLocalDateConverter.INSTANCE);
		converters.add(RowToBooleanConverter.INSTANCE);
		converters.add(RowToInstantConverter.INSTANCE);
		converters.add(RowToDateConverter.INSTANCE);
		converters.add(RowToInetAddressConverter.INSTANCE);
		converters.add(RowToListConverter.INSTANCE);
		converters.add(RowToMapConverter.INSTANCE);
		converters.add(RowToNumberConverterFactory.INSTANCE);
		converters.add(RowToStringConverter.INSTANCE);
		converters.add(RowToUuidConverter.INSTANCE);

		return converters;
	}

	@ReadingConverter
	public enum RowToBooleanConverter implements Converter<Row, Boolean> {

		INSTANCE;

		@Override
		public Boolean convert(Row row) {
			return row.getBoolean(0);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link Date} representation.
	 *
	 * @author Mark Paluch
	 */
	@ReadingConverter
	public enum RowToDateConverter implements Converter<Row, Date> {

		INSTANCE;

		@Override
		public Date convert(Row row) {
			return Date.from(row.getInstant(0));
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link Instant} representation.
	 *
	 * @author Mark Paluch
	 */
	@ReadingConverter
	public enum RowToInstantConverter implements Converter<Row, Instant> {

		INSTANCE;

		@Override
		public Instant convert(Row row) {
			return row.getInstant(0);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link InetAddress} representation.
	 *
	 * @author Mark Paluch
	 */
	@ReadingConverter
	public enum RowToInetAddressConverter implements Converter<Row, InetAddress> {

		INSTANCE;

		@Override
		public InetAddress convert(Row row) {
			return row.getInetAddress(0);
		}
	}

	/**
	 * Singleton converter factory to convert the first column of a {@link Row} to a {@link Number}.
	 * <p>
	 * Support Number classes including Byte, Short, Integer, Float, Double, Long, BigInteger, BigDecimal. This class
	 * delegates to {@link NumberUtils#convertNumberToTargetClass(Number, Class)} to perform the conversion.
	 *
	 * @see Byte
	 * @see Short
	 * @see Integer
	 * @see Long
	 * @see java.math.BigInteger
	 * @see Float
	 * @see Double
	 * @see java.math.BigDecimal
	 */
	@ReadingConverter
	public enum RowToNumberConverterFactory implements ConverterFactory<Row, Number> {

		INSTANCE;

		@Override
		public <T extends Number> Converter<Row, T> getConverter(Class<T> targetType) {
			Assert.notNull(targetType, "Target type must not be null");
			return new RowToNumber<>(targetType);
		}

		private static final class RowToNumber<T extends Number> implements Converter<Row, T> {

			private final Class<T> targetType;

			RowToNumber(Class<T> targetType) {
				this.targetType = targetType;
			}

			@Override
			public T convert(Row source) {

				Object object = source.getObject(0);

				return (object != null ? NumberUtils.convertNumberToTargetClass((Number) object, this.targetType) : null);
			}
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link String} representation.
	 *
	 * @author Mark Paluch
	 */
	@ReadingConverter
	public enum RowToStringConverter implements Converter<Row, String> {

		INSTANCE;

		@Override
		public String convert(Row row) {
			return row.getString(0);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link UUID} representation.
	 *
	 * @author Mark Paluch
	 */
	@ReadingConverter
	public enum RowToUuidConverter implements Converter<Row, UUID> {

		INSTANCE;

		@Override
		public UUID convert(Row row) {
			return row.getUuid(0);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their Cassandra {@link LocalDate} representation.
	 *
	 * @author Mark Paluch
	 */
	@ReadingConverter
	public enum RowToCassandraLocalDateConverter implements Converter<Row, LocalDate> {

		INSTANCE;

		@Override
		public LocalDate convert(Row row) {
			return row.getLocalDate(0);
		}
	}
}
