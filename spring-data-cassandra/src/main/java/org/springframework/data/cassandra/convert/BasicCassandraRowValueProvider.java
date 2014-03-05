/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.convert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.util.Assert;

import com.datastax.driver.core.Row;

/**
 * {@link PropertyValueProvider} to read property values from a {@link Row}.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David Webb
 */
public class BasicCassandraRowValueProvider implements CassandraRowValueProvider {

	private final ColumnReader reader;
	private final SpELExpressionEvaluator evaluator;

	/**
	 * Creates a new {@link BasicCassandraRowValueProvider} with the given {@link Row} and
	 * {@link DefaultSpELExpressionEvaluator}.
	 * 
	 * @param source must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public BasicCassandraRowValueProvider(Row source, DefaultSpELExpressionEvaluator evaluator) {
		Assert.notNull(source);
		Assert.notNull(evaluator);

		this.reader = new ColumnReader(source);
		this.evaluator = evaluator;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object getPropertyValue(CassandraPersistentProperty property) {

		String expression = property.getSpelExpression();
		if (expression != null) {
			return evaluator.evaluate(expression);
		}

		return reader.get(property.getColumnName());
	}

	@Override
	public Row getRow() {
		return reader.getRow();
	}
}
