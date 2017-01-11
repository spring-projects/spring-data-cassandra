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

import static org.assertj.core.api.Assertions.*;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.WritingConverter;

import com.datastax.driver.core.Row;

/**
 * Unit tests for {@link CustomConversions}.
 *
 * @soundtrack Atc - Why Oh Why (Extended Version)
 * @author Mark Paluch
 */
public class CustomConversionsUnitTests {

	@Test // DATACASS-280
	public void findsBasicReadAndWriteConversions() {

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(FormatToStringConverter.INSTANCE, StringToFormatConverter.INSTANCE));

		assertThat(conversions.getCustomWriteTarget(Format.class, null)).isAssignableFrom(String.class);
		assertThat(conversions.getCustomWriteTarget(String.class, null)).isNull();

		assertThat(conversions.hasCustomReadTarget(String.class, Format.class)).isTrue();
		assertThat(conversions.hasCustomReadTarget(String.class, Locale.class)).isFalse();
	}

	@Test // DATACASS-280
	public void considersSubtypesCorrectly() {

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(NumberToStringConverter.INSTANCE, StringToNumberConverter.INSTANCE));

		assertThat(conversions.getCustomWriteTarget(Long.class, null)).isAssignableFrom(String.class);
		assertThat(conversions.hasCustomReadTarget(String.class, Long.class)).isTrue();
	}

	@Test // DATACASS-280
	public void considersTypesWeRegisteredConvertersForAsSimple() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(FormatToStringConverter.INSTANCE));
		assertThat(conversions.isSimpleType(UUID.class)).isTrue();
	}

	@Test // DATACASS-280
	public void populatesConversionServiceCorrectly() {

		GenericConversionService conversionService = new DefaultConversionService();

		CustomConversions conversions = new CustomConversions(Arrays.asList(StringToFormatConverter.INSTANCE));
		conversions.registerConvertersIn(conversionService);

		assertThat(conversionService.canConvert(String.class, Format.class)).isTrue();
	}

	@Test // DATACASS-280
	public void doesNotConsiderTypeSimpleIfOnlyReadConverterIsRegistered() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(StringToFormatConverter.INSTANCE));
		assertThat(conversions.isSimpleType(Format.class)).isFalse();
	}

	@Test // DATACASS-280
	public void discoversConvertersForSubtypesOfCassandraTypes() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(StringToIntegerConverter.INSTANCE));
		assertThat(conversions.hasCustomReadTarget(String.class, Integer.class)).isTrue();
		assertThat(conversions.hasCustomWriteTarget(String.class, Integer.class)).isTrue();
	}

	@Test // DATACASS-280
	public void considersUUIDASimpleType() {

		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.isSimpleType(UUID.class)).isTrue();
	}

	@Test // DATACASS-280
	public void considersInetAddressASimpleType() {

		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.isSimpleType(InetAddress.class)).isTrue();
	}

	@Test // DATACASS-280
	public void considersRowASimpleType() {

		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.isSimpleType(Row.class)).isTrue();
	}

	@Test // DATACASS-280
	@SuppressWarnings("rawtypes")
	public void favorsCustomConverterForIndeterminedTargetType() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(DateTimeToStringConverter.INSTANCE));
		assertThat(conversions.getCustomWriteTarget(DateTime.class, null)).isEqualTo((Class) String.class);
	}

	@Test // DATACASS-280
	public void customConverterOverridesDefault() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(CustomDateTimeConverter.INSTANCE));
		GenericConversionService conversionService = new DefaultConversionService();
		conversions.registerConvertersIn(conversionService);

		assertThat(conversionService.convert(new DateTime(), Date.class)).isEqualTo(new Date(0));
	}

	@Test // DATACASS-280
	public void shouldSelectPropertCustomWriteTargetForCglibProxiedType() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(FormatToStringConverter.INSTANCE));
		assertThat(conversions.getCustomWriteTarget(createProxyTypeFor(Format.class))).isAssignableFrom(String.class);
	}

	@Test // DATACASS-280
	public void shouldSelectPropertyCustomReadTargetForCglibProxiedType() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(CustomObjectToStringConverter.INSTANCE));
		assertThat(conversions.hasCustomReadTarget(createProxyTypeFor(Object.class), String.class)).isTrue();
	}

	@Test // DATACASS-280
	public void registersConverterFactoryCorrectly() {

		CustomConversions customConversions = new CustomConversions(
				Collections.singletonList(new FormatConverterFactory()));

		assertThat(customConversions.getCustomWriteTarget(String.class, SimpleDateFormat.class)).isNotNull();
	}

	@Test // DATACASS-296
	public void registersConvertersForJsr310() {

		CustomConversions customConversions = new CustomConversions();

		assertThat(customConversions.hasCustomWriteTarget(java.time.LocalDateTime.class)).isTrue();
	}

	@Test // DATACASS-296
	public void registersConvertersForThreeTenBackPort() {

		CustomConversions customConversions = new CustomConversions();

		assertThat(customConversions.hasCustomWriteTarget(org.threeten.bp.LocalDateTime.class)).isTrue();
	}

	@Test // DATACASS-296
	public void registersConvertersForJoda() {

		CustomConversions customConversions = new CustomConversions();

		assertThat(customConversions.hasCustomWriteTarget(org.joda.time.LocalDate.class)).isTrue();
	}

	private static Class<?> createProxyTypeFor(Class<?> type) {

		ProxyFactory factory = new ProxyFactory();
		factory.setProxyTargetClass(true);
		factory.setTargetClass(type);

		return factory.getProxy().getClass();
	}

	enum FormatToStringConverter implements Converter<Format, String> {
		INSTANCE;

		public String convert(Format source) {
			return source.toString();
		}
	}

	enum StringToFormatConverter implements Converter<String, Format> {
		INSTANCE;
		public Format convert(String source) {
			return DateFormat.getInstance();
		}
	}

	enum NumberToStringConverter implements Converter<Number, String> {
		INSTANCE;
		public String convert(Number source) {
			return source.toString();
		}
	}

	enum StringToNumberConverter implements Converter<String, Number> {
		INSTANCE;
		public Number convert(String source) {
			return 0L;
		}
	}

	enum StringToIntegerConverter implements Converter<String, Integer> {
		INSTANCE;
		public Integer convert(String source) {
			return 0;
		}
	}

	enum DateTimeToStringConverter implements Converter<DateTime, String> {
		INSTANCE;

		@Override
		public String convert(DateTime source) {
			return "";
		}
	}

	enum CustomDateTimeConverter implements Converter<DateTime, Date> {

		INSTANCE;

		@Override
		public Date convert(DateTime source) {
			return new Date(0);
		}
	}

	enum CustomObjectToStringConverter implements Converter<Object, String> {

		INSTANCE;

		@Override
		public String convert(Object source) {
			return source != null ? source.toString() : null;
		}

	}

	@WritingConverter
	static class FormatConverterFactory implements ConverterFactory<String, Format> {

		@Override
		public <T extends Format> Converter<String, T> getConverter(Class<T> targetType) {
			return new StringToFormat<T>(targetType);
		}

		private static final class StringToFormat<T extends Format> implements Converter<String, T> {

			private final Class<T> targetType;

			public StringToFormat(Class<T> targetType) {
				this.targetType = targetType;
			}

			@Override
			public T convert(String source) {

				if (source.length() == 0) {
					return null;
				}

				try {
					return targetType.newInstance();
				} catch (Exception e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}
			}
		}
	}
}
