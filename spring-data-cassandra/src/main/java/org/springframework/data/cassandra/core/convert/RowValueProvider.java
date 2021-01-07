/*
 * Copyright 2013-2021 the original author or authors.
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

import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * {@link CassandraValueProvider} to read property values from a {@link Row}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public class RowValueProvider implements CassandraValueProvider {

	private final RowReader reader;

	private final SpELExpressionEvaluator evaluator;

	/**
	 * Create a new {@link RowValueProvider} with the given {@link Row}, {@link CodecRegistry} and
	 * {@link SpELExpressionEvaluator}.
	 *
	 * @param source must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public RowValueProvider(Row source, SpELExpressionEvaluator evaluator) {

		Assert.notNull(source, "Source Row must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		this.reader = new RowReader(source);
		this.evaluator = evaluator;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.model.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
	 */
	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	public <T> T getPropertyValue(CassandraPersistentProperty property) {

		String spelExpression = property.getSpelExpression();

		return spelExpression != null ? this.evaluator.evaluate(spelExpression)
				: (T) this.reader.get(property.getRequiredColumnName());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraValueProvider#hasProperty(org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty)
	 */
	@Override
	public boolean hasProperty(CassandraPersistentProperty property) {

		Assert.notNull(property, "CassandraPersistentProperty must not be null");

		return this.reader.contains(property.getRequiredColumnName());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraValueProvider#getSource()
	 */
	@Override
	public Object getSource() {
		return this.reader.getRow();
	}
}
