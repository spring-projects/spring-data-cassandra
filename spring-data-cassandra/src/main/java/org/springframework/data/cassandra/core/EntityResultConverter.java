/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.cassandra.core;

import com.datastax.oss.driver.api.core.cql.Row;

enum EntityResultConverter implements QueryResultConverter<Object, Object> {

	INSTANCE;

	@Override
	public Object mapRow(Row row, ConversionResultSupplier<Object> reader) {
		return reader.get();
	}

	@Override
	public <V> QueryResultConverter<Object, V> andThen(QueryResultConverter<? super Object, ? extends V> after) {
		return (QueryResultConverter) after;
	}
}
