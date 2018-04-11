/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.DataType.Name;

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

	/**
	 * Set of types that indicate usage of Cassandra's time type. Time is represented as long so we need to imply the type
	 * from an artificial simple type.
	 */
	private final static Set<Class<?>> NATIVE_TIME_TYPE_MARKERS;

	private static final StoreConversions STORE_CONVERSIONS;

	static {

		List<Object> converters = new ArrayList<>();

		converters.addAll(CassandraConverters.getConvertersToRegister());
		converters.addAll(CassandraJodaTimeConverters.getConvertersToRegister());
		converters.addAll(CassandraJsr310Converters.getConvertersToRegister());
		converters.addAll(CassandraThreeTenBackPortConverters.getConvertersToRegister());

		STORE_CONVERTERS = Collections.unmodifiableList(converters);
		STORE_CONVERSIONS = StoreConversions.of(CassandraSimpleTypeHolder.HOLDER, STORE_CONVERTERS);

		List<? extends Class<?>> timeMarkers = STORE_CONVERTERS.stream() //
				.filter(Converter.class::isInstance) //
				.map(Object::getClass) //
				.filter(it -> AnnotatedElementUtils.hasAnnotation(it, WritingConverter.class)) //
				.filter(it -> {

					CassandraType annotation = AnnotatedElementUtils.getMergedAnnotation(it, CassandraType.class);

					return annotation != null && annotation.type() == Name.TIME;
				}) //
				.map(it -> {

					ResolvableType classType = ResolvableType.forClass(it).as(Converter.class).getGeneric(0);

					return classType.getRawClass();
				})
				.collect(Collectors.toList());

		NATIVE_TIME_TYPE_MARKERS = new HashSet<>(timeMarkers);
	}

	/**
	 * Create a new {@link CassandraCustomConversions} instance registering the given converters.
	 *
	 * @param converters must not be {@literal null}.
	 */
	public CassandraCustomConversions(List<?> converters) {
		super(STORE_CONVERSIONS, converters);
	}

	/**
	 * Returns {@literal true} if the {@link Class type} is used to denote Cassandra's {@code time} column type.
	 *
	 * @param type must not be {@literal null}.
	 * @return {@literal true} if the type maps to Cassandra's {@code time} column type.
	 * @since 2.1
	 */
	public boolean isNativeTimeTypeMarker(Class<?> type) {

		Assert.notNull(type, "Type must not be null");

		return NATIVE_TIME_TYPE_MARKERS.contains(ClassUtils.getUserClass(type));
	}
}
