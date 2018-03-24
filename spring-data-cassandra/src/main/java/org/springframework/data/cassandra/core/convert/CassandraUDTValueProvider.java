/*
 * Copyright 2016-2018 the original author or authors.
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

import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;

/**
 * {@link CassandraValueProvider} to read property values from a {@link UDTValue}.
 *
 * @author Mark Paluch
 * @since 1.5
 */
public class CassandraUDTValueProvider implements CassandraValueProvider {

	private final UDTValue udtValue;

	private final CodecRegistry codecRegistry;

	private final SpELExpressionEvaluator evaluator;

	/**
	 * Create a new {@link CassandraUDTValueProvider} with the given {@link UDTValue} and {@link SpELExpressionEvaluator}.
	 *
	 * @param udtValue must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 * @since 2.1
	 */
	public CassandraUDTValueProvider(UDTValue udtValue, CodecRegistry codecRegistry,
			SpELExpressionEvaluator evaluator) {

		Assert.notNull(udtValue, "UDTValue must not be null");
		Assert.notNull(codecRegistry, "CodecRegistry must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		this.udtValue = udtValue;
		this.codecRegistry = codecRegistry;
		this.evaluator = evaluator;
	}

	/**
	 * Create a new {@link CassandraUDTValueProvider} with the given {@link UDTValue} and
	 * {@link DefaultSpELExpressionEvaluator}.
	 *
	 * @param udtValue must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 * @deprecated since 2.1, use {@link #CassandraUDTValueProvider(UDTValue, CodecRegistry, SpELExpressionEvaluator)}
	 */
	@Deprecated
	public CassandraUDTValueProvider(UDTValue udtValue, CodecRegistry codecRegistry,
			DefaultSpELExpressionEvaluator evaluator) {

		this(udtValue, codecRegistry, (SpELExpressionEvaluator) evaluator);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	public <T> T getPropertyValue(CassandraPersistentProperty property) {

		String spelExpression = property.getSpelExpression();

		if (spelExpression != null) {
			return this.evaluator.evaluate(spelExpression);
		}

		String name = property.getRequiredColumnName().toCql();
		DataType fieldType = this.udtValue.getType().getFieldType(name);

		return this.udtValue.get(name, this.codecRegistry.codecFor(fieldType));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraValueProvider#hasProperty(org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty)
	 */
	@Override
	public boolean hasProperty(CassandraPersistentProperty property) {
		return this.udtValue.getType().contains(property.getRequiredColumnName().toCql());
	}
}
