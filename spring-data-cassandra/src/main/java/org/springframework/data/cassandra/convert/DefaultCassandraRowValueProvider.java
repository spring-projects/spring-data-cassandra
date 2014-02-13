/*
 * Copyright 2012 the original author or authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * {@link PropertyValueProvider} to read property values from a {@link Row}.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author David Webb
 */
public class DefaultCassandraRowValueProvider implements CassandraRowValueProvider {

	private static Logger log = LoggerFactory.getLogger(DefaultCassandraRowValueProvider.class);

	private final Row source;
	private final SpELExpressionEvaluator evaluator;

	/**
	 * Creates a new {@link DefaultCassandraRowValueProvider} with the given {@link Row} and
	 * {@link DefaultSpELExpressionEvaluator}.
	 * 
	 * @param source must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public DefaultCassandraRowValueProvider(Row source, DefaultSpELExpressionEvaluator evaluator) {
		Assert.notNull(source);
		Assert.notNull(evaluator);

		this.source = source;
		this.evaluator = evaluator;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getPropertyValue(CassandraPersistentProperty property) {

		String expression = property.getSpelExpression();
		if (expression != null) {
			return evaluator.evaluate(expression);
		}

		String columnName = property.getColumnName();
		if (source.isNull(property.getColumnName())) {
			return null;
		}
		DataType columnType = source.getColumnDefinitions().getType(columnName);

		log.debug(columnType.getName().name());

		// TODO DW Set, Map, List

		if (columnType.equals(DataType.text()) || columnType.equals(DataType.ascii())
				|| columnType.equals(DataType.varchar())) {
			return (T) source.getString(columnName);
		}
		if (columnType.equals(DataType.cint()) || columnType.equals(DataType.varint())) {
			return (T) new Integer(source.getInt(columnName));
		}
		if (columnType.equals(DataType.cdouble())) {
			return (T) new Double(source.getDouble(columnName));
		}
		if (columnType.equals(DataType.bigint()) || columnType.equals(DataType.counter())) {
			return (T) new Long(source.getLong(columnName));
		}
		if (columnType.equals(DataType.cfloat())) {
			return (T) new Float(source.getFloat(columnName));
		}
		if (columnType.equals(DataType.decimal())) {
			return (T) source.getDecimal(columnName);
		}
		if (columnType.equals(DataType.cboolean())) {
			return (T) new Boolean(source.getBool(columnName));
		}
		if (columnType.equals(DataType.timestamp())) {
			return (T) source.getDate(columnName);
		}
		if (columnType.equals(DataType.blob())) {
			return (T) source.getBytes(columnName);
		}
		if (columnType.equals(DataType.inet())) {
			return (T) source.getInet(columnName);
		}
		if (columnType.equals(DataType.uuid()) || columnType.equals(DataType.timeuuid())) {
			return (T) source.getUUID(columnName);
		}

		return (T) source.getBytes(columnName);
	}

	public Row getRow() {
		return source;
	}
}
