/*
 * Copyright 2016-2021 the original author or authors.
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

import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * {@link CassandraValueProvider} to read property values from a {@link UdtValue}.
 *
 * @author Mark Paluch
 * @since 1.5
 * @deprecated since 3.0, use {@link UdtValueProvider} directly.
 */
@Deprecated
public class CassandraUDTValueProvider extends UdtValueProvider {

	/**
	 * Create a new {@link CassandraUDTValueProvider} with the given {@link UdtValue} and {@link SpELExpressionEvaluator}.
	 *
	 * @param udtValue must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 * @since 2.1
	 */
	public CassandraUDTValueProvider(UdtValue udtValue, CodecRegistry codecRegistry, SpELExpressionEvaluator evaluator) {
		super(udtValue, evaluator);
	}

	/**
	 * Create a new {@link CassandraUDTValueProvider} with the given {@link UDTValue} and
	 * {@link DefaultSpELExpressionEvaluator}.
	 *
	 * @param udtValue must not be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 * @deprecated since 2.1, use {@link #CassandraUDTValueProvider(UdtValue, CodecRegistry, SpELExpressionEvaluator)}
	 */
	@Deprecated
	public CassandraUDTValueProvider(UdtValue udtValue, CodecRegistry codecRegistry,
			DefaultSpELExpressionEvaluator evaluator) {
		super(udtValue, evaluator);
	}
}
