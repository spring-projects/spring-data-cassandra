/*
 * Copyright 2016-present the original author or authors.
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
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.data.UdtValue;

/**
 * {@link CassandraValueProvider} to read property values from a {@link UdtValue}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public class UdtValueProvider implements CassandraValueProvider {

	private final UdtValue udtValue;

	private final ValueExpressionEvaluator evaluator;

	/**
	 * Create a new {@link UdtValueProvider} with the given {@link UdtValue} and {@link ValueExpressionEvaluator}.
	 *
	 * @param udtValue must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public UdtValueProvider(UdtValue udtValue, ValueExpressionEvaluator evaluator) {

		Assert.notNull(udtValue, "UDTValue must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		this.udtValue = udtValue;
		this.evaluator = evaluator;
	}

	public <T> @Nullable T getPropertyValue(CassandraPersistentProperty property) {

		String spelExpression = property.getSpelExpression();

		if (spelExpression != null) {
			return this.evaluator.evaluate(spelExpression);
		}

		return (T) this.udtValue.getObject(property.getRequiredColumnName());
	}

	@Override
	public boolean hasProperty(CassandraPersistentProperty property) {
		return this.udtValue.getType().contains(property.getRequiredColumnName());
	}

	@Override
	public Object getSource() {
		return this.udtValue;
	}

}
