/*
 * Copyright 2018-2025 the original author or authors.
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

import org.springframework.data.mapping.model.SpELExpressionEvaluator;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * {@link CassandraValueProvider} to read property values from a {@link TupleValue}.
 *
 * @author Mark Paluch
 * @since 2.1
 * @deprecated since 3.0, use {@link TupleValueProvider} directly.
 */
@Deprecated
public class CassandraTupleValueProvider extends TupleValueProvider {

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
		super(tupleValue, evaluator);
	}
}
