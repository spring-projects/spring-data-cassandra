/*
 * Copyright 2016-2020 the original author or authors.
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
import org.springframework.data.mapping.model.PropertyValueProvider;

/**
 * {@link PropertyValueProvider} for {@link CassandraPersistentProperty}. This {@link PropertyValueProvider} allows
 * querying whether the source contains a data source for {@link CassandraPersistentProperty} like a field or a column.
 *
 * @author Mark Paluch
 * @since 1.5
 */
public interface CassandraValueProvider extends PropertyValueProvider<CassandraPersistentProperty> {

	/**
	 * Returns whether the underlying source contains a data source for the given {@link CassandraPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @return {@literal true} if the underlying source contains a data source for the given
	 *         {@link CassandraPersistentProperty}.
	 */
	boolean hasProperty(CassandraPersistentProperty property);

	/**
	 * Returns whether the underlying source.
	 *
	 * @return the underlying source for this {@link CassandraValueProvider}.
	 * @since 3.0
	 */
	Object getSource();
}
