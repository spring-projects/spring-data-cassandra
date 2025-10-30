/*
 * Copyright 2023-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * {@link ValueConversionContext} that allows to delegate read/write to an underlying {@link CassandraConverter}.
 *
 * @author Mark Paluch
 * @since 4.2
 */
public class CassandraConversionContext implements ValueConversionContext<CassandraPersistentProperty> {

	private final PropertyValueProvider<CassandraPersistentProperty> accessor;
	private final CassandraPersistentProperty persistentProperty;
	private final CassandraConverter cassandraConverter;

	private final @Nullable SpELContext spELContext;

	public CassandraConversionContext(PropertyValueProvider<CassandraPersistentProperty> accessor,
			CassandraPersistentProperty persistentProperty, CassandraConverter CassandraConverter) {
		this(accessor, persistentProperty, CassandraConverter, null);
	}

	public CassandraConversionContext(PropertyValueProvider<CassandraPersistentProperty> accessor,
			CassandraPersistentProperty persistentProperty, CassandraConverter CassandraConverter,
			@Nullable SpELContext spELContext) {

		this.accessor = accessor;
		this.persistentProperty = persistentProperty;
		this.cassandraConverter = CassandraConverter;
		this.spELContext = spELContext;
	}

	@Override
	public CassandraPersistentProperty getProperty() {
		return persistentProperty;
	}

	public @Nullable Object getValue(String propertyPath) {
		return accessor.getPropertyValue(persistentProperty.getOwner().getRequiredPersistentProperty(propertyPath));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> @Nullable T write(@Nullable Object value, TypeInformation<T> target) {

		if (value == null) {
			return null;
		}

		return (T) cassandraConverter.convertToColumnType(value, target);
	}

	@Override
	public <T> @Nullable T read(@Nullable Object value, TypeInformation<T> target) {
		return value instanceof Row row ? cassandraConverter.read(target.getType(), row)
				: ValueConversionContext.super.read(value, target);
	}

	public @Nullable SpELContext getSpELContext() {
		return spELContext;
	}

}
