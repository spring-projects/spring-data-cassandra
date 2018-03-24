/*
 * Copyright 2018 the original author or authors.
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
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleValue;

/**
 * {@link CassandraValueProvider} to read property values from a {@link TupleValue}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public class CassandraTupleValueProvider implements CassandraValueProvider {

	private final CodecRegistry codecRegistry;

	private final SpELExpressionEvaluator evaluator;

	private final TupleValue tupleValue;

	/**
	 * Create a new {@link CassandraTupleValueProvider} with the given {@link TupleValue} and
	 * {@link SpELExpressionEvaluator}.
	 *
	 * @param tupleValue must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public CassandraTupleValueProvider(TupleValue tupleValue, CodecRegistry codecRegistry,
			SpELExpressionEvaluator evaluator) {

		Assert.notNull(tupleValue, "TupleValue must not be null");
		Assert.notNull(codecRegistry, "CodecRegistry must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		this.tupleValue = tupleValue;
		this.codecRegistry = codecRegistry;
		this.evaluator = evaluator;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
	 */
	@Nullable
	@Override
	public <T> T getPropertyValue(CassandraPersistentProperty property) {

		String spelExpression = property.getSpelExpression();

		if (spelExpression != null) {
			return evaluator.evaluate(spelExpression);
		}

		int ordinal = property.getRequiredOrdinal();
		DataType elementType = tupleValue.getType().getComponentTypes().get(ordinal);

		return tupleValue.get(ordinal, codecRegistry.codecFor(elementType));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraValueProvider#hasProperty(org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty)
	 */
	@Override
	public boolean hasProperty(CassandraPersistentProperty property) {
		return this.tupleValue.getType().getComponentTypes().size() >= property.getRequiredOrdinal();
	}
}
