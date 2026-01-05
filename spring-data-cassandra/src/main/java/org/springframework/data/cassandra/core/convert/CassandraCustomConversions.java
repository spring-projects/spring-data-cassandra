/*
 * Copyright 2017-present the original author or authors.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder;
import org.springframework.data.convert.ConverterBuilder;
import org.springframework.data.convert.Jsr310Converters;
import org.springframework.data.convert.PropertyValueConversions;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.convert.PropertyValueConverterRegistrar;
import org.springframework.data.convert.SimplePropertyValueConversions;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.Assert;

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
		converters.addAll(CassandraJsr310Converters.getConvertersToRegister());

		STORE_CONVERTERS = Collections.unmodifiableList(converters);
		STORE_CONVERSIONS = StoreConversions.of(CassandraSimpleTypeHolder.HOLDER, STORE_CONVERTERS);
	}

	/**
	 * Create a new {@link CassandraCustomConversions} instance registering the given converters.
	 *
	 * @param converters must not be {@literal null}.
	 */
	public CassandraCustomConversions(List<?> converters) {
		super(new CassandraConverterConfiguration(converters));
	}

	/**
	 * Create a new {@link CassandraCustomConversions} given {@link CassandraConverterConfigurationAdapter}.
	 *
	 * @param conversionConfiguration must not be {@literal null}.
	 * @since 4.2
	 */
	protected CassandraCustomConversions(CassandraConverterConfigurationAdapter conversionConfiguration) {
		super(conversionConfiguration.createConverterConfiguration());
	}

	/**
	 * Functional style {@link org.springframework.data.convert.CustomConversions} creation giving users a convenient way
	 * of configuring store specific capabilities by providing deferred hooks to what will be configured when creating the
	 * {@link org.springframework.data.convert.CustomConversions#CustomConversions(ConverterConfiguration) instance}.
	 *
	 * @param configurer must not be {@literal null}.
	 * @since 4.2
	 */
	public static CassandraCustomConversions create(Consumer<CassandraConverterConfigurationAdapter> configurer) {

		CassandraConverterConfigurationAdapter adapter = new CassandraConverterConfigurationAdapter();
		configurer.accept(adapter);

		return new CassandraCustomConversions(adapter);
	}

	/**
	 * Cassandra-specific extension to {@link org.springframework.data.convert.CustomConversions.ConverterConfiguration}.
	 * This extension avoids {@link Converter} registrations that enforce date mapping to {@link Date} from JSR-310.
	 */
	static class CassandraConverterConfiguration extends ConverterConfiguration {

		CassandraConverterConfiguration(List<?> converters) {
			super(STORE_CONVERSIONS, converters, getConverterFilter());
		}

		CassandraConverterConfiguration(List<?> userConverters, PropertyValueConversions propertyValueConversions) {
			super(STORE_CONVERSIONS, userConverters, getConverterFilter(), propertyValueConversions);
		}

		static Predicate<ConvertiblePair> getConverterFilter() {

			return convertiblePair -> !(Jsr310Converters.supports(convertiblePair.getSourceType())
					&& Date.class.isAssignableFrom(convertiblePair.getTargetType()));
		}
	}

	/**
	 * {@link CassandraConverterConfigurationAdapter} encapsulates creation of
	 * {@link org.springframework.data.convert.CustomConversions.ConverterConfiguration} with Cassandra specifics.
	 *
	 * @author Mark Paluch
	 * @since 4.2
	 */
	public static class CassandraConverterConfigurationAdapter {

		private final List<Object> customConverters = new ArrayList<>();

		private final PropertyValueConversions internalValueConversion = PropertyValueConversions.simple(it -> {});
		private PropertyValueConversions propertyValueConversions = internalValueConversion;

		/**
		 * Create a {@link CassandraConverterConfigurationAdapter} using the provided {@code converters} and our own codecs
		 * for JSR-310 types.
		 *
		 * @param converters must not be {@literal null}.
		 * @return
		 */
		public static CassandraConverterConfigurationAdapter from(List<?> converters) {

			Assert.notNull(converters, "Converters must not be null");

			CassandraConverterConfigurationAdapter adapter = new CassandraConverterConfigurationAdapter();
			adapter.registerConverters(converters);

			return adapter;
		}

		/**
		 * Add a custom {@link Converter} implementation.
		 *
		 * @param converter must not be {@literal null}.
		 * @return this.
		 */
		public CassandraConverterConfigurationAdapter registerConverter(Converter<?, ?> converter) {

			Assert.notNull(converter, "Converter must not be null");

			customConverters.add(converter);
			return this;
		}

		/**
		 * Add a custom {@link ConverterFactory} implementation.
		 *
		 * @param converterFactory must not be {@literal null}.
		 * @return this.
		 */
		public CassandraConverterConfigurationAdapter registerConverterFactory(ConverterFactory<?, ?> converterFactory) {

			Assert.notNull(converterFactory, "ConverterFactory must not be null");

			customConverters.add(converterFactory);
			return this;
		}

		/**
		 * Add {@link Converter converters}, {@link ConverterFactory factories}, {@link ConverterBuilder.ConverterAware
		 * converter-aware objects}, and {@link GenericConverter generic converters}.
		 *
		 * @param converters must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 */
		public CassandraConverterConfigurationAdapter registerConverters(Object... converters) {
			return registerConverters(Arrays.asList(converters));
		}

		/**
		 * Add {@link Converter converters}, {@link ConverterFactory factories}, {@link ConverterBuilder.ConverterAware
		 * converter-aware objects}, and {@link GenericConverter generic converters}.
		 *
		 * @param converters must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 */
		public CassandraConverterConfigurationAdapter registerConverters(Collection<?> converters) {

			Assert.notNull(converters, "Converters must not be null");
			Assert.noNullElements(converters, "Converters must not be null nor contain null values");

			customConverters.addAll(converters);
			return this;
		}

		/**
		 * Add a custom/default {@link PropertyValueConverterFactory} implementation used to serve
		 * {@link PropertyValueConverter}.
		 *
		 * @param converterFactory must not be {@literal null}.
		 * @return this.
		 */
		public CassandraConverterConfigurationAdapter registerPropertyValueConverterFactory(
				PropertyValueConverterFactory converterFactory) {

			Assert.state(valueConversions() instanceof SimplePropertyValueConversions,
					"Configured PropertyValueConversions does not allow setting custom ConverterRegistry");

			((SimplePropertyValueConversions) valueConversions()).setConverterFactory(converterFactory);
			return this;
		}

		/**
		 * Gateway to register property specific converters.
		 *
		 * @param configurationAdapter must not be {@literal null}.
		 * @return this.
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public CassandraConverterConfigurationAdapter configurePropertyConversions(
				Consumer<PropertyValueConverterRegistrar<CassandraPersistentProperty>> configurationAdapter) {

			Assert.state(valueConversions() instanceof SimplePropertyValueConversions,
					"Configured PropertyValueConversions does not allow setting custom ConverterRegistry");

			PropertyValueConverterRegistrar propertyValueConverterRegistrar = new PropertyValueConverterRegistrar();
			configurationAdapter.accept(propertyValueConverterRegistrar);

			((SimplePropertyValueConversions) valueConversions())
					.setValueConverterRegistry(propertyValueConverterRegistrar.buildRegistry());
			return this;
		}

		/**
		 * Optionally set the {@link PropertyValueConversions} to be applied during mapping.
		 * <p>
		 * Use this method if {@link #configurePropertyConversions(Consumer)} and
		 * {@link #registerPropertyValueConverterFactory(PropertyValueConverterFactory)} are not sufficient.
		 *
		 * @param valueConversions must not be {@literal null}.
		 * @return this.
		 */
		public CassandraConverterConfigurationAdapter withPropertyValueConversions(
				PropertyValueConversions valueConversions) {

			Assert.notNull(valueConversions, "PropertyValueConversions must not be null");

			this.propertyValueConversions = valueConversions;
			return this;
		}

		PropertyValueConversions valueConversions() {

			if (this.propertyValueConversions == null) {
				this.propertyValueConversions = internalValueConversion;
			}

			return this.propertyValueConversions;
		}

		CassandraConverterConfiguration createConverterConfiguration() {

			if (hasDefaultPropertyValueConversions()
					&& propertyValueConversions instanceof SimplePropertyValueConversions svc) {
				svc.init();
			}

			return new CassandraConverterConfiguration(this.customConverters, this.propertyValueConversions);
		}

		private boolean hasDefaultPropertyValueConversions() {
			return propertyValueConversions == internalValueConversion;
		}
	}
}
