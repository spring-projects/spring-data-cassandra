/*
 * Copyright 2022-present the original author or authors.
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

import org.springframework.data.convert.PropertyValueConverter;

/**
 * Cassandra-specific {@link PropertyValueConverter} extension. Converters can implement this interface for
 * Cassandra-specific value conversions, for example:
 *
 * <pre class="code">
 * static class MyJsonConverter implements CassandraValueConverter&lt;Person, String&gt; {
 *
 * 	&#64;Override
 * 	public Person read(String value, CassandraConversionContext context) {
 * 		return // decode JSON to Person object
 * 	}
 *
 * 	&#64;Override
 * 	public String write(Person value, CassandraConversionContext context) {
 * 		return // marshal Person to JSON
 * 	}
 * }
 * </pre>
 *
 * @author Mark Paluch
 * @since 4.2
 * @see org.springframework.data.convert.ValueConverter
 */
public interface CassandraValueConverter<S, T> extends PropertyValueConverter<S, T, CassandraConversionContext> {}
