/*
 * Copyright 2017-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder;
import org.springframework.data.convert.Jsr310Converters;
import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * Value object to capture custom conversion. {@link CassandraCustomConversions} also act as factory for
 * {@link SimpleTypeHolder}
 *
 * @author Mark Paluch
 * @since 2.0
 * @see org.springframework.data.convert.CustomConversions
 * @see SimpleTypeHolder
 */
public class CassandraCustomConversions extends org.springframework.data.convert.CustomConversions {

	private static final List<Object> STORE_CONVERTERS;

	private static final StoreConversions STORE_CONVERSIONS;

	static {

		List<Object> converters = new ArrayList<>();

		converters.addAll(CassandraConverters.getConvertersToRegister());
		converters.addAll(CassandraJodaTimeConverters.getConvertersToRegister());
		converters.addAll(CassandraJsr310Converters.getConvertersToRegister());
		converters.addAll(CassandraThreeTenBackPortConverters.getConvertersToRegister());

		STORE_CONVERTERS = Collections.unmodifiableList(converters);
		STORE_CONVERSIONS = StoreConversions.of(CassandraSimpleTypeHolder.HOLDER, STORE_CONVERTERS);
	}

	/**
	 * Create a new {@link CassandraCustomConversions} instance registering the given converters.
	 *
	 * @param converters must not be {@literal null}.
	 */
	public CassandraCustomConversions(List<?> converters) {
		super(new CassandraConverterConfiguration(STORE_CONVERSIONS, converters));
	}

	/**
	 * Cassandra-specific extension to {@link org.springframework.data.convert.CustomConversions.ConverterConfiguration}.
	 * This extension avoids {@link Converter} registrations that enforce date mapping to {@link Date} from JSR-310, Joda
	 * Time and ThreeTenBackport.
	 */
	static class CassandraConverterConfiguration extends ConverterConfiguration {

		public CassandraConverterConfiguration(StoreConversions storeConversions, List<?> userConverters) {
			super(storeConversions, userConverters, getConverterFilter());
		}

		static Predicate<ConvertiblePair> getConverterFilter() {

			return convertiblePair -> {

				if (sourceMatches(convertiblePair, "org.joda.time") || sourceMatches(convertiblePair, "org.threeten.bp")
						|| Jsr310Converters.supports(convertiblePair.getSourceType())
								&& Date.class.isAssignableFrom(convertiblePair.getTargetType())) {
					return false;
				}

				return true;
			};
		}

		private static boolean sourceMatches(ConvertiblePair convertiblePair, String packagePrefix) {
			return convertiblePair.getSourceType().getName().startsWith(packagePrefix);
		}
	}
}
